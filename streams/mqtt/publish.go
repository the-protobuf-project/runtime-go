package mqtt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/packets"
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
	o := streams.NewOptions(opts...)
	topic, props, err := m.prepare(ctx, subject, o)
	if err != nil {
		return "", err
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
		Topic:      topic,
		QoS:        m.store.cfg.qos,
		Retain:     o.Retain,
		Properties: props,
		Payload:    body,
	}); err != nil {
		m.store.log.Error(ctx, "could not publish", err, telemetry.Fields{"subject": subject, "id": id})
		return "", fmt.Errorf("mqtt: cannot publish on %q: %w", subject, err)
	}

	m.store.log.Debug(ctx, "published", telemetry.Fields{"subject": subject, "id": id, "bytes": len(body)})
	return id, nil
}

// prepare returns the topic a publish on subject goes to and the PUBLISH
// properties o asks for, or why nothing can be published there with o.
//
// The properties carry the stream's headers with the publish's own on top as
// MQTT 5 user properties, and [streams.Options.Expires] as the Message Expiry
// Interval. A publish with neither sends no properties at all, byte for byte
// what this package sent before headers existed.
func (m *manager) prepare(ctx context.Context, subject string, o streams.Options) (string, *paho.PublishProperties, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return "", nil, err
	}
	if hasWildcard(subject) {
		// A stream may declare a filter, but a message has to land somewhere
		// specific. The broker would reject this; saying so here names the call.
		return "", nil, fmt.Errorf("%w: cannot publish to the wildcard subject %q, only subscribe to it (%w)", streams.ErrUnknownSubject, subject, streams.ErrUnsupported)
	}
	if o.TTL > 0 {
		return "", nil, fmt.Errorf("%w: MQTT delivers on publish and cannot schedule", streams.ErrUnsupported)
	}
	topic := m.store.topic(m.stream.ID, subject)
	if hasWildcard(topic) {
		return "", nil, fmt.Errorf("%w: the topic %q carries a wildcard from the stream id or prefix, and nothing can be published on it", streams.ErrUnsupported, topic)
	}
	headers, err := core.Outgoing(m.stream, o)
	if err != nil {
		return "", nil, err
	}
	return topic, publishProperties(headers, o.Expires), nil
}

// maxExpiry is the longest Message Expiry Interval MQTT 5 can carry, in
// seconds.
const maxExpiry = math.MaxUint32

// expiryInterval is d as MQTT 5's Message Expiry Interval: whole seconds,
// rounded up, so a message is never dropped sooner than asked. A sub-second
// expiry is one second, never zero; one beyond MaxUint32 seconds (136 years)
// is capped there.
func expiryInterval(d time.Duration) uint32 {
	secs := uint64(d / time.Second)
	if d%time.Second != 0 {
		secs++
	}
	return uint32(min(max(secs, 1), maxExpiry))
}

// publishProperties are the PUBLISH properties for headers and expires: the
// headers as user properties in key order, the expiry as the Message Expiry
// Interval. Nil when there is neither.
//
// The broker, and any MQTT 5 tool on it, sees the headers. The broker drops a message that has waited longer than its
// expiry in an offline session's queue or as a retained value; one handed to
// a connected subscriber at once is delivered whatever its expiry.
func publishProperties(headers streams.Headers, expires time.Duration) *paho.PublishProperties {
	if len(headers) == 0 && expires <= 0 {
		return nil
	}
	props := &paho.PublishProperties{}
	for _, k := range headers.Keys() {
		props.User.Add(k, headers[k])
	}
	if expires > 0 {
		interval := expiryInterval(expires)
		props.MessageExpiry = &interval
	}
	return props
}

// headersOf is the headers a PUBLISH arrived with: its MQTT 5 user
// properties. MQTT allows a key more than once, which [streams.Headers] does
// not; the last one wins, as a key a publish sets over its stream's does.
func headersOf(p *paho.Publish) streams.Headers {
	if p.Properties == nil || len(p.Properties.User) == 0 {
		return nil
	}
	h := make(streams.Headers, len(p.Properties.User))
	for _, u := range p.Properties.User {
		h[u.Key] = u.Value
	}
	return h
}

// send publishes once a connection is up, and again on the next one when the
// connection dropped between the two.
//
// Both errors it retries mean the packet never reached the wire — autopaho had
// no client, or paho had no connection to assign it a packet id on — so trying
// again is not a duplicate. A drop after the packet was sent is not retried
// here: the session carries it, and it is sent again on reconnect.
func (m *manager) send(ctx context.Context, p *paho.Publish) error {
	cm, _ := m.store.link(p.Topic)
	for {
		if err := cm.AwaitConnection(ctx); err != nil {
			return fmt.Errorf("not connected: %w", err)
		}
		_, err := cm.Publish(ctx, p)
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
// MQTT has no batch primitive, and at QoS 1 each message is acknowledged on
// its own, but nothing makes the second wait for the first's PUBACK: the whole
// batch is put in flight on the topic's publishing connection, in order, up to
// the broker's Receive Maximum, and the call returns when every
// acknowledgement is in or the publish timeout passes. A refused entry fails
// alone, reported by its index as [core.BatchError] does everywhere.
//
// On a connection [Use] was handed, and at QoS 0, the batch is published one
// message at a time instead.
func (m *manager) PublishBatch(ctx context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	o := streams.NewOptions(opts...)
	if err := core.CheckBatch(o); err != nil {
		return nil, err
	}
	// An undeclared subject, a wildcard, a delay or a bad header fails every
	// entry the same way; say so once, as Publish does, rather than as a batch
	// of identical failures.
	topic, props, err := m.prepare(ctx, subject, o)
	if err != nil {
		return nil, err
	}
	cm, pipe := m.store.link(topic)
	if pipe == nil || m.store.cfg.qos == 0 {
		return core.PublishEach(ctx, m, subject, values, opts...)
	}

	ids := make([]string, len(values))
	errs := make([]error, len(values))
	var pkts []*packets.Publish
	var index []int
	for i, value := range values {
		id := core.NewID()
		body, perr := core.Pack(m.store.codec, id, value)
		if perr != nil {
			errs[i] = perr
			continue
		}
		ids[i] = id
		p := &paho.Publish{Topic: topic, QoS: m.store.cfg.qos, Retain: o.Retain, Properties: props, Payload: body}
		pkts = append(pkts, p.Packet())
		index = append(index, i)
	}

	// Waiting for the connection is ctx's to bound, as for Publish; the
	// timeout starts once the batch can go on the wire.
	if err := cm.AwaitConnection(ctx); err != nil {
		return nil, fmt.Errorf("mqtt: cannot publish on %q: not connected: %w", subject, err)
	}
	bctx, cancel := context.WithTimeout(ctx, m.store.cfg.publishTimeout)
	defer cancel()
	for n, perr := range pipe.sendAll(bctx, cm, pkts) {
		errs[index[n]] = perr
	}

	var failures []error
	for i, perr := range errs {
		if perr != nil {
			ids[i] = ""
			failures = append(failures, fmt.Errorf("entry %d: %w", i, perr))
		}
	}
	if len(failures) > 0 {
		m.store.log.Error(ctx, "could not publish every message of a batch", failures[0], telemetry.Fields{
			"subject": subject, "failed": len(failures), "total": len(values),
		})
	} else {
		m.store.log.Debug(ctx, "published a batch", telemetry.Fields{"subject": subject, "count": len(values)})
	}
	return ids, core.BatchError(subject, len(values), failures)
}

// pipeline is a publishing connection's session state, wrapped so a batch can
// be put on the wire whole and acknowledged together.
//
// paho's Publish writes a message and then waits for its PUBACK, so a batch
// through it is one round trip per message, and running those concurrently
// would let them reach the broker in any order. pipeline does what Publish
// does in two halves: every message is added to the session (which allocates
// its packet id, stores it for redelivery and takes one of the broker's
// Receive Maximum slots) and written, one after another, by one goroutine;
// then every acknowledgement is awaited. The session is paho's own in-memory
// one, so it still matches each PUBACK to its packet and sends again whatever
// was not acknowledged when a connection drops.
type pipeline struct {
	session.SessionManager

	mu   sync.Mutex
	conn io.Writer
}

// ConAckReceived records the connection the session writes on.
func (p *pipeline) ConAckReceived(conn io.Writer, cp *packets.Connect, ca *packets.Connack) error {
	if err := p.SessionManager.ConAckReceived(conn, cp, ca); err != nil {
		return err
	}
	p.mu.Lock()
	p.conn = conn
	p.mu.Unlock()
	return nil
}

// ConnectionLost forgets the connection, which is no longer written on.
func (p *pipeline) ConnectionLost(dp *packets.Disconnect) error {
	p.mu.Lock()
	p.conn = nil
	p.mu.Unlock()
	return p.SessionManager.ConnectionLost(dp)
}

// sendAll adds every packet to the session and writes it, in order, then
// waits for each to be acknowledged. Outcomes are in packet order.
//
// A packet the session took but that could not be written (the connection
// dropped between the two) is not failed: the session sends it again on the
// next connection, and its acknowledgement settles it then, or ctx ends.
func (p *pipeline) sendAll(ctx context.Context, cm *autopaho.ConnectionManager, pkts []*packets.Publish) []error {
	errs := make([]error, len(pkts))
	acks := make([]chan packets.ControlPacket, len(pkts))
	for i, pkt := range pkts {
		ack := make(chan packets.ControlPacket, 1)
		for {
			err := p.AddToSession(ctx, pkt, ack)
			if err == nil {
				acks[i] = ack
				break
			}
			if !errors.Is(err, session.ErrNoConnection) {
				errs[i] = err
				break
			}
			// The connection dropped mid-batch. The rest wait for the next
			// one, in order, rather than failing into the outage.
			if werr := cm.AwaitConnection(ctx); werr != nil {
				errs[i] = fmt.Errorf("not connected: %w", werr)
				break
			}
		}
		if acks[i] == nil {
			continue
		}
		p.mu.Lock()
		conn := p.conn
		p.mu.Unlock()
		if conn != nil {
			_, _ = pkt.WriteTo(conn)
		}
	}
	for i, ack := range acks {
		if ack == nil {
			continue
		}
		select {
		case resp := <-ack:
			errs[i] = acknowledged(resp)
		case <-ctx.Done():
			errs[i] = fmt.Errorf("the broker did not acknowledge in time: %w", ctx.Err())
		}
	}
	return errs
}

// acknowledged is the outcome the broker's answer to one publish means.
func acknowledged(resp packets.ControlPacket) error {
	switch r := resp.Content.(type) {
	case *packets.Puback:
		if r.ReasonCode >= 0x80 {
			return fmt.Errorf("the broker refused the publish: %s", r.Reason())
		}
		return nil
	case *packets.Pubrec:
		if r.ReasonCode >= 0x80 {
			return fmt.Errorf("the broker refused the publish: %s", r.Reason())
		}
		return nil
	case *packets.Pubcomp:
		return nil
	}
	return errors.New("the connection closed before the broker acknowledged")
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
