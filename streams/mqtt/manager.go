package mqtt

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/eclipse/paho.golang/paho"
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
)

// checkSubject rejects a subject the stream does not declare.
//
// An MQTT topic filter is a pattern, so a stream declaring "user/+" accepts
// "user/created" — matched with DeclaresPattern, on MQTT's own wildcards.
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

	if _, err := m.store.client.Publish(ctx, &paho.Publish{
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

// Subscribe returns a channel of messages for a subject.
//
// This is the undurable half: a clean session that keeps nothing, so a message
// published while nobody is attached is gone. Reach for [manager.Consume] when
// that matters.
func (m *manager) Subscribe(ctx context.Context, subject string) (<-chan streams.Message, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}

	msgs, _, err := m.attach(ctx, subject, "", "runtime-go-sub-"+core.NewID(), false)
	if err != nil {
		return nil, err
	}

	out := make(chan streams.Message)
	go func() {
		defer close(out)
		for d := range msgs {
			select {
			case out <- d.Message:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// Consume delivers messages under a named consumer.
//
// The name is an MQTT session: the broker keeps that client's subscriptions and
// its unacknowledged messages while it is away, so a consumer that dies and
// comes back is handed what it missed and what it never finished. That is what
// makes the name, rather than the process, the thing with a position.
//
// Two processes cannot share one name — MQTT gives a client id to one session,
// and a second connection under it takes the first one's place. Pass
// [streams.Group] to split work instead: that subscribes through a shared
// subscription, where the broker distributes messages among the members and
// each keeps a session of its own.
//
// # Acknowledging is a packet on the wire
//
// [streams.Delivery.Ack] sends a PUBACK, and canceling ctx in the same breath
// can close the connection before it is written. The message is then still
// outstanding and the broker delivers it again — at-least-once behaving as
// promised, but a duplicate a consumer that shut down cleanly would not have
// seen. Let the consumer keep running for a moment after its last
// acknowledgement.
//
// # There is no attempt count
//
// MQTT marks a repeat delivery with a duplicate flag but does not say how many
// times it has tried, so [streams.Delivery.Attempt] is zero here — the
// contract's answer where a provider cannot count.
func (m *manager) Consume(ctx context.Context, subject, consumer string, opts ...streams.Option) (<-chan streams.Delivery, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}
	if consumer == "" {
		return nil, fmt.Errorf("%w: a durable consumer needs a name, because the name is the session that survives a restart", streams.ErrUnsupported)
	}

	o := streams.NewOptions(opts...)

	// Without a group the name is the session, and it is one consumer. With
	// one, the name still identifies this member's session and the group is
	// what the broker shares the subject among — so each member needs an id of
	// its own, or the second would take the first's session away.
	clientID := consumerID(consumer, subject)
	if o.Group != "" {
		clientID += "-" + core.NewID()
	}

	msgs, client, err := m.attach(ctx, subject, o.Group, clientID, true)
	if err != nil {
		return nil, err
	}

	m.store.log.Info(ctx, "consuming", telemetry.Fields{
		"subject": subject, "consumer": consumer, "group": o.Group, "client_id": clientID,
	})

	out := make(chan streams.Delivery)
	go func() {
		defer close(out)
		defer func() { _ = client.Disconnect(&paho.Disconnect{ReasonCode: 0}) }()

		delivered := 0
		defer func() {
			m.store.log.Info(ctx, "consumer stopped", telemetry.Fields{
				"subject": subject, "consumer": consumer, "delivered": delivered,
			})
		}()

		for d := range msgs {
			delivered++
			select {
			// MQTT flags a repeat delivery but does not count them, so
			// Attempt is zero — the contract's answer where a provider cannot.
			case out <- streams.NewDelivery(d.Message, 0, &mqttAck{client: client, packet: d.packet, subject: subject}):
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// received is one delivery and the packet it has to be acknowledged against.
type received struct {
	streams.Message
	packet *paho.Publish
}

// attach opens a connection, subscribes, and returns the messages arriving on
// it. The connection is closed when ctx is done.
func (m *manager) attach(ctx context.Context, subject, group, clientID string, durable bool) (<-chan received, *paho.Client, error) {
	topic := m.store.topic(m.stream.ID, subject)
	filter := topic
	if group != "" {
		// A shared subscription: the broker gives each message to one member
		// rather than to all of them.
		filter = "$share/" + group + "/" + topic
	}

	// Buffered so a slow reader does not stall the client's read loop, which
	// would block every other subscription on this connection.
	raw := make(chan received, 64)

	client, resumed, err := m.store.dial(ctx, clientID, durable, func(pr paho.PublishReceived) (bool, error) {
		msg, derr := core.Unpack(m.store.registry, subject, pr.Packet.Payload)
		if derr != nil {
			// One bad message is not a reason to tear down a healthy
			// subscription, but it must be acknowledged or a durable session
			// would be handed it again forever.
			m.store.log.Warn(ctx, "acknowledging a malformed message",
				telemetry.Fields{"subject": subject, "error": derr.Error()})
			_ = pr.Client.Ack(pr.Packet)
			// Reporting the decode failure back to paho would be reporting it
			// to the wrong place: this is one bad payload, not a fault in the
			// connection, and returning it here is what would tear the
			// subscription down.
			return true, nil //nolint:nilerr // handled by acknowledging it
		}
		select {
		case raw <- received{Message: msg, packet: pr.Packet}:
		case <-ctx.Done():
		}
		return true, nil
	})
	if err != nil {
		return nil, nil, err
	}

	// A resumed session comes back with its subscriptions already restored, so
	// subscribing again is not just redundant — it races the messages the
	// broker is redelivering, and a broker that shares one packet-identifier
	// space across both directions rejects it as an identifier already in use.
	//
	// This is safe because the client id identifies a consumer *and* a subject
	// (see consumerID): a session that exists is a session already subscribed
	// to exactly this filter.
	if resumed {
		m.store.log.Debug(ctx, "resumed a session; its subscription was restored",
			telemetry.Fields{"subject": subject, "client_id": clientID})
		go func() {
			<-ctx.Done()
			close(raw)
		}()
		return raw, client, nil
	}

	suback, err := client.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: m.store.cfg.qos}},
	})
	if err != nil {
		_ = client.Disconnect(&paho.Disconnect{ReasonCode: 0})
		// The broker's reason code says why, and paho's error text does not
		// carry it — a refused filter and a quota exceeded look identical
		// without this.
		if suback != nil && len(suback.Reasons) > 0 {
			return nil, nil, fmt.Errorf("mqtt: cannot subscribe to %q (broker reason %d): %w",
				subject, suback.Reasons[0], err)
		}
		return nil, nil, fmt.Errorf("mqtt: cannot subscribe to %q: %w", subject, err)
	}

	// Closing the channel on cancellation is what ends the delivery goroutines
	// reading from it.
	go func() {
		<-ctx.Done()
		close(raw)
	}()

	return raw, client, nil
}

// consumerID is the client id a named consumer of one subject connects under.
//
// It is derived from both, and deterministically, for two reasons. Stable,
// because the client id *is* the session: generate a fresh one and every
// restart is a stranger the broker kept nothing for. And per subject, because
// MQTT restores a session's subscriptions wholesale — one session per subject
// is what makes "this session already exists" mean "already subscribed to
// exactly this", which is what lets a resumed consumer skip subscribing.
//
// The subject is hashed rather than appended: brokers cap client id length, and
// a subject is arbitrarily long.
func consumerID(consumer, subject string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(subject))
	return fmt.Sprintf("%s-%08x", safeName(consumer), h.Sum32())
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

// PublishBatch sends several values on a subject.
//
// MQTT has no batch primitive; at QoS 1 each message is acknowledged on its own.
func (m *manager) PublishBatch(ctx context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	if err := core.CheckBatch(streams.NewOptions(opts...)); err != nil {
		return nil, err
	}
	return core.PublishEach(ctx, m, subject, values, opts...)
}

// mqttAck settles one delivery with a PUBACK.
type mqttAck struct {
	client  *paho.Client
	packet  *paho.Publish
	subject string
}

func (a *mqttAck) Ack(context.Context) error {
	if err := a.client.Ack(a.packet); err != nil {
		return fmt.Errorf("mqtt: cannot acknowledge on %q: %w", a.subject, err)
	}
	return nil
}

// Nak leaves the message unacknowledged: the broker keeps it against this
// session and delivers it again when the consumer reconnects. MQTT has no
// negative acknowledgement, so there is nothing more truthful to do here.
func (a *mqttAck) Nak(context.Context) error { return nil }
