package mqtt

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/eclipse/paho.golang/paho/session"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// manager publishes to and subscribes from one stream.
type manager struct {
	store  *store
	stream streams.Stream
}

var (
	_ streams.Manager = (*manager)(nil)
	_ streams.Durable = (*manager)(nil)
	_ streams.Batch   = (*manager)(nil)
)

// checkSubject rejects a subject the stream does not declare.
//
// An MQTT topic filter is a pattern, so a stream declaring "user/+" accepts
// "user/created" — matched on MQTT's own wildcards.
func (m *manager) checkSubject(ctx context.Context, subject string) error {
	if declares(m.stream.Subjects, subject) {
		return nil
	}
	m.store.log.Error(ctx, "subject is not declared by this stream", nil, telemetry.Fields{
		"subject": subject, "stream": m.stream.ID, "declared": m.stream.Subjects,
	})
	return core.ErrSubject(m.stream.ID, subject, m.stream.Subjects)
}

// Publish sends a value on a subject.
//
// While the connection is down and being dialed again, Publish waits for it
// for as long as ctx allows — give ctx a deadline to bound that, or a publish
// during a broker outage waits out the outage.
func (m *manager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return "", err
	}
	if hasWildcard(subject) {
		// A stream may declare a filter, but a message has to land somewhere
		// specific. The broker would reject this; saying so here names the call.
		return "", fmt.Errorf("%w: cannot publish to the wildcard subject %q, only subscribe to it", streams.ErrUnknownSubject, subject)
	}

	o := streams.NewOptions(opts...)
	if o.TTL > 0 {
		return "", fmt.Errorf("%w: MQTT delivers on publish and cannot schedule", streams.ErrUnsupported)
	}

	id := o.ID
	if id == "" {
		id = core.NewID()
	}

	body, err := core.Pack(m.store.codec, id, value)
	if err != nil {
		return "", err
	}

	if err := m.send(ctx, &paho.Publish{
		Topic:   m.store.topic(m.stream.ID, subject),
		QoS:     m.store.cfg.qos,
		Payload: body,
	}); err != nil {
		m.store.log.Error(ctx, "could not publish", err, telemetry.Fields{"subject": subject, "id": id})
		return "", fmt.Errorf("mqtt: cannot publish on %q: %w", subject, err)
	}

	m.store.log.Debug(ctx, "published", telemetry.Fields{"subject": subject, "id": id, "bytes": len(body)})
	return id, nil
}

// send publishes once a connection is up, and again on the next one when the
// connection dropped between the two.
//
// Both errors it retries mean the packet never reached the wire — autopaho had
// no client, or paho had no connection to assign it a packet id on — so trying
// again is not a duplicate. A drop after the packet was sent is not retried
// here: the session carries it, and it is sent again on reconnect.
func (m *manager) send(ctx context.Context, p *paho.Publish) error {
	for {
		if err := m.store.pub.AwaitConnection(ctx); err != nil {
			return fmt.Errorf("not connected: %w", err)
		}
		_, err := m.store.pub.Publish(ctx, p)
		if !errors.Is(err, autopaho.ConnectionDownError) && !errors.Is(err, session.ErrNoConnection) {
			return err
		}
		// autopaho may not have noticed the drop yet, in which case it still
		// reports the old connection as up; give it a moment to.
		select {
		case <-ctx.Done():
			return fmt.Errorf("not connected: %w", ctx.Err())
		case <-time.After(redialPause):
		}
	}
}

// redialPause is how long a publish that found the connection gone waits
// before checking for the next one.
const redialPause = 50 * time.Millisecond

// PublishBatch sends several values on a subject.
//
// MQTT has no batch primitive; at QoS 1 each message is acknowledged on its own.
func (m *manager) PublishBatch(ctx context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	if err := core.CheckBatch(streams.NewOptions(opts...)); err != nil {
		return nil, err
	}
	return core.PublishEach(ctx, m, subject, values, opts...)
}

// declares reports whether any declared subject matches, on MQTT's wildcards.
func declares(declared []string, subject string) bool {
	for _, d := range declared {
		if matches(d, subject) {
			return true
		}
	}
	return false
}
