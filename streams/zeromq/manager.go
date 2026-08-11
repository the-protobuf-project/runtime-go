package zeromq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// manager publishes to and subscribes from one stream.
type manager struct {
	store  *store
	stream streams.Stream
}

var _ streams.Manager = (*manager)(nil)

// checkSubject rejects a subject the stream does not declare.
//
// A ZeroMQ subscription is a byte prefix rather than a pattern, so a subject is
// matched by name — as on Redis and Kafka.
func (m *manager) checkSubject(ctx context.Context, subject string) error {
	if core.Declares(m.stream.Subjects, subject) {
		return nil
	}
	m.store.log.Error(ctx, "subject is not declared by this stream", nil, telemetry.Fields{
		"subject": subject, "stream": m.stream.ID, "declared": m.stream.Subjects,
	})
	return core.ErrSubject(m.stream.ID, subject, m.stream.Subjects)
}

// envelope is the wire form: the topic frame first, so a SUB socket can filter
// on it without decoding the payload, then the packed message.
//
// Two frames rather than one prefixed blob because ZeroMQ filters on the first
// frame's leading bytes, and a topic that is a whole frame cannot be a prefix
// of a longer one by accident — "user" would otherwise match "users".
func (m *manager) topic(subject string) string {
	return m.stream.ID + "|" + subject
}

// Publish sends a value on a subject.
//
// It returns [streams.ErrUnsupported] on a provider built by [Subscribe]: a SUB
// socket cannot send, and refusing beats accepting a message that would go
// nowhere.
func (m *manager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return "", err
	}
	if m.store.pub == nil {
		return "", fmt.Errorf("%w: this provider was built with Subscribe and can only receive; use Publish to bind an endpoint", streams.ErrUnsupported)
	}

	o := streams.NewOptions(opts...)
	if o.TTL > 0 {
		return "", fmt.Errorf("%w: ZeroMQ delivers on send and cannot schedule", streams.ErrUnsupported)
	}
	if o.Group != "" {
		// PUB/SUB fans out; every subscriber gets every message. Sharing a
		// subject would need a different socket pair (PUSH/PULL), which is a
		// different topology rather than an option on this one.
		return "", fmt.Errorf("%w: ZeroMQ PUB/SUB fans out to every subscriber and cannot share a subject", streams.ErrUnsupported)
	}

	id := o.ID
	if id == "" {
		id = core.NewID()
	}

	body, err := core.Pack(m.store.codec, id, value)
	if err != nil {
		return "", err
	}

	if err := m.store.pub.SendMulti(zmq4.NewMsgFrom([]byte(m.topic(subject)), body)); err != nil {
		m.store.log.Error(ctx, "could not publish", err, telemetry.Fields{"subject": subject, "id": id})
		return "", fmt.Errorf("zeromq: cannot publish on %q: %w", subject, err)
	}

	m.store.log.Debug(ctx, "published", telemetry.Fields{"subject": subject, "id": id, "bytes": len(body)})
	return id, nil
}

// Subscribe returns a channel of messages for a subject.
//
// The channel is closed when ctx is done, which is also the only way to release
// the socket.
//
// A value published in the moment after this returns may still be missed; see
// the package documentation on the slow-joiner problem, and [WithSettle].
func (m *manager) Subscribe(ctx context.Context, subject string) (<-chan streams.Message, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}

	sock := zmq4.NewSub(ctx)
	if err := sock.Dial(m.store.endpoint); err != nil {
		_ = sock.Close()
		return nil, fmt.Errorf("zeromq: cannot connect to %s: %w", m.store.endpoint, err)
	}

	topic := m.topic(subject)
	if err := sock.SetOption(zmq4.OptionSubscribe, topic); err != nil {
		_ = sock.Close()
		return nil, fmt.Errorf("zeromq: cannot subscribe to %q: %w", subject, err)
	}
	m.store.track(sock)

	// The subscription is on its way to the publisher and nothing will confirm
	// it arrived. Waiting narrows the window; see the package documentation.
	select {
	case <-time.After(m.store.cfg.settle):
	case <-ctx.Done():
		_ = sock.Close()
		return nil, ctx.Err()
	}

	m.store.log.Info(ctx, "subscribed", telemetry.Fields{
		"subject": subject, "endpoint": m.store.endpoint, "topic": topic,
	})

	out := make(chan streams.Message)
	go func() {
		defer close(out)

		// Recv blocks with no context of its own, so cancellation has to reach
		// it by closing the socket underneath it.
		stop := context.AfterFunc(ctx, func() { _ = sock.Close() })
		defer stop()

		delivered := 0
		defer func() {
			m.store.log.Info(ctx, "subscription closed",
				telemetry.Fields{"subject": subject, "delivered": delivered})
		}()

		for {
			msg, err := sock.Recv()
			if err != nil {
				// A closed socket is how a canceled subscription ends.
				if ctx.Err() == nil && !errors.Is(err, context.Canceled) {
					m.store.log.Error(ctx, "could not receive", err,
						telemetry.Fields{"subject": subject})
				}
				return
			}
			if len(msg.Frames) < 2 {
				m.store.log.Warn(ctx, "dropping a message with no payload frame",
					telemetry.Fields{"subject": subject, "frames": len(msg.Frames)})
				continue
			}

			decoded, derr := core.Unpack(m.store.registry, subject, msg.Frames[1])
			if derr != nil {
				// One bad message is not a reason to tear down a healthy
				// subscription.
				m.store.log.Warn(ctx, "dropping a malformed message",
					telemetry.Fields{"subject": subject, "error": derr.Error()})
				continue
			}

			delivered++
			select {
			case out <- decoded:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
