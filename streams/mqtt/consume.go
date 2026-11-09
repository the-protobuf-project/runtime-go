package mqtt

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"strings"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Subscribe returns a channel of messages for a subject.
//
// This is the undurable half: the broker keeps nothing for it, so a message
// published while nobody is attached — including while the connection is being
// dialed again after a drop — is gone. Reach for [manager.Consume] when that
// matters.
//
// [streams.Group] subscribes through a shared subscription, so the members
// split the subject rather than each receiving all of it.
func (m *manager) Subscribe(ctx context.Context, subject string, opts ...streams.Option) (<-chan streams.Message, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}

	o := streams.NewOptions(opts...)
	raw, cm, err := m.attach(ctx, subject, o.Group, "runtime-go-sub-"+core.NewID(), 0, 0)
	if err != nil {
		return nil, err
	}

	out := make(chan streams.Message, core.Prefetch(o))
	go func() {
		defer close(out)
		defer func() { _ = m.store.disconnect(cm) }()
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-raw:
				select {
				case out <- d.Message:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// Consume delivers messages under a named consumer.
//
// The name is an MQTT session: the broker keeps that client's subscriptions and
// its unacknowledged messages while it is away, so a consumer that dies — or
// whose connection drops and is dialed again — is handed what it missed and
// what it never finished. That is what makes the name, rather than the process,
// the thing with a position.
//
// Two processes cannot share one name — MQTT gives a client id to one session,
// and a second connection under it takes the first one's place. Pass
// [streams.Group] to split work instead: that subscribes through a shared
// subscription, where the broker distributes messages among the members and
// each keeps a session of its own.
//
// # Acknowledging is a packet on the wire
//
// [streams.Delivery.Ack] sends a PUBACK on the connection the message arrived
// on. If that connection has dropped since, the acknowledgement fails and the
// broker delivers the message again on the new one; canceling ctx in the same
// breath as acknowledging can lose it the same way. Both are at-least-once
// behaving as promised, and both are duplicates a consumer should expect.
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

	raw, cm, err := m.attach(ctx, subject, o.Group, clientID, m.store.cfg.expiry, core.Prefetch(o))
	if err != nil {
		return nil, err
	}

	m.store.log.Info(ctx, "consuming", telemetry.Fields{
		"subject": subject, "consumer": consumer, "group": o.Group, "client_id": clientID,
	})

	out := make(chan streams.Delivery, core.Prefetch(o))
	go func() {
		defer close(out)
		defer func() { _ = m.store.disconnect(cm) }()

		delivered := 0
		defer func() {
			m.store.log.Info(ctx, "consumer stopped", telemetry.Fields{"subject": subject, "consumer": consumer, "delivered": delivered})
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-raw:
				delivered++
				m.store.metrics.Delivered(ctx, subject, consumer)
				ack := &mqttAck{received: d, metrics: m.store.metrics, subject: subject, consumer: consumer}
				select {
				case out <- streams.NewDelivery(d.Message, 0, ack):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// attach opens a connection subscribed to subject and returns the messages
// arriving on it, across every reconnection.
//
// The channel is never closed. Paho calls the handler from its own goroutine
// and may still be inside it when ctx ends, so closing it would race that send;
// readers stop on ctx instead.
//
// A non-zero expiry makes the session durable: the broker keeps it across a
// drop and a restart, and the first connection resumes one it already holds.
// A durable session also tells the broker prefetch as its Receive Maximum, and
// is handed a retained value only when its subscription is new, not again on
// every resume. An undurable one acknowledges each message on receipt, since
// nothing would ever acknowledge it otherwise and the broker would stop
// sending once its in-flight window filled.
//
// A message's headers are its MQTT 5 user properties, and on a wildcard
// subscription its subject is the topic it arrived on below the stream's
// prefix, so a subscription to "user/+" reports "user/created".
func (m *manager) attach(ctx context.Context, subject, group, clientID string, expiry uint32, prefetch int) (<-chan received, *autopaho.ConnectionManager, error) {
	filter := m.store.topic(m.stream.ID, subject)
	if group != "" {
		// A shared subscription: the broker gives each message to one member
		// rather than to all of them.
		filter = "$share/" + group + "/" + filter
	}

	// Buffered so a slow reader does not stall the client's read loop, which
	// would block every other subscription on this connection.
	raw := make(chan received, 64)

	durable := expiry > 0
	var tune tuning
	if durable {
		tune = tuning{receiveMax: uint16(min(max(prefetch, 1), math.MaxUint16)), retainHandling: 1}
	}
	prefix := m.store.topic(m.stream.ID, "")
	wildcard := hasWildcard(subject)

	cm, err := m.store.open(ctx, clientID, durable, expiry, filter, func(pr paho.PublishReceived) (bool, error) {
		if len(pr.Packet.Payload) == 0 {
			// A retained value being cleared: no frame, nothing to deliver.
			// Settled at once, or a durable session would be handed it again
			// forever.
			m.store.log.Debug(ctx, "skipping an empty message (a retained value cleared)", telemetry.Fields{"subject": subject})
			_ = pr.Client.Ack(pr.Packet)
			return true, nil
		}
		if !durable {
			_ = pr.Client.Ack(pr.Packet)
		}
		on := subject
		if wildcard && strings.HasPrefix(pr.Packet.Topic, prefix) {
			on = strings.TrimPrefix(pr.Packet.Topic, prefix)
		}
		msg, derr := core.Unpack(m.store.registry, on, pr.Packet.Payload)
		if derr != nil {
			// One bad message is not a reason to tear down a healthy
			// subscription, but it must be acknowledged or a durable session
			// would be handed it again forever.
			m.store.log.Warn(ctx, "acknowledging a malformed message",
				telemetry.Fields{"subject": subject, "error": derr.Error()})
			if durable {
				_ = pr.Client.Ack(pr.Packet)
			}
			// Reporting the decode failure back to paho would be reporting it
			// to the wrong place: this is one bad payload, not a fault in the
			// connection, and returning it here is what would tear the
			// subscription down.
			return true, nil
		}
		if headers := headersOf(pr.Packet); headers != nil {
			msg.Headers = msg.Headers.Merged(headers)
		}
		select {
		case raw <- received{Message: msg, client: pr.Client, packet: pr.Packet}:
		case <-ctx.Done():
		}
		return true, nil
	}, tune)
	if err != nil {
		return nil, nil, err
	}
	return raw, cm, nil
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
