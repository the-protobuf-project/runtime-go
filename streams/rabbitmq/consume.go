package rabbitmq

import (
	"context"
	"fmt"
	"math"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Subscribe returns a channel of messages for a subject.
//
// The queue behind it is exclusive and auto-deleting: it belongs to this
// subscription and is removed when the connection drops, so nothing piles up
// for a subscriber that has gone away. After a drop a new one is bound in its
// place, and what was published in between is gone — that is what makes this
// the undurable half. Reach for [manager.Consume] when a message must survive
// the reader.
func (m *manager) Subscribe(ctx context.Context, subject string, opts ...streams.Option) (<-chan streams.Message, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}
	attach := func() (attachment, error) { return m.attach(ctx, subject, "", true) }
	a, err := attach()
	if err != nil {
		return nil, err
	}

	m.store.log.Info(ctx, "subscribed", telemetry.Fields{"subject": subject})

	out := make(chan streams.Message, core.Prefetch(streams.NewOptions(opts...)))
	go func() {
		defer close(out)
		m.follow(ctx, subject, a, attach, func(d amqp.Delivery) bool {
			msg, derr := core.Unpack(m.store.registry, d.RoutingKey, d.Body)
			if derr != nil {
				m.store.log.Warn(ctx, "dropping a malformed message", telemetry.Fields{"subject": subject, "error": derr.Error()})
				_ = d.Reject(false)
				return true
			}
			select {
			case out <- msg:
				// Acknowledged once handed over, which is what lets prefetch
				// bound how much the broker pushes ahead of the reader.
				_ = d.Ack(false)
				return true
			case <-ctx.Done():
				return false
			}
		})
	}()
	return out, nil
}

// Consume delivers messages under a named consumer.
//
// The name is a durable queue bound to the stream's exchange for this subject.
// It is the broker that holds the queue, so it outlives the process reading it:
// messages published while nobody is consuming — or while the connection is
// being dialed again — are waiting when the name comes back, and a message
// taken by a consumer that dies unacknowledged is given to whoever is next.
//
// Two processes under one name take from that one queue and so split the work.
// That is AMQP's round-robin rather than anything arranged here, which is why
// [streams.Group] is refused — the name is already the group.
//
// # Nak is real here
//
// RabbitMQ has a true negative acknowledgement, so [streams.Delivery.Nak]
// returns a message for immediate redelivery rather than waiting out a
// visibility timeout as on Redis, or doing nothing as on Kafka.
//
// # Settling after a reconnect
//
// A delivery is settled on the channel it arrived on. Once the connection has
// dropped that channel is gone, so Ack and Nak fail, and the broker — which
// requeued everything unacknowledged when the connection went — delivers the
// message again, flagged as a redelivery.
func (m *manager) Consume(ctx context.Context, subject, consumer string, opts ...streams.Option) (<-chan streams.Delivery, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}
	if consumer == "" {
		return nil, fmt.Errorf("%w: a durable consumer needs a name, because the name is the queue that survives a restart", streams.ErrUnsupported)
	}
	o := streams.NewOptions(opts...)
	if o.Group != "" {
		return nil, fmt.Errorf("%w: the consumer name %q is already the queue on RabbitMQ, and consumers of one queue already share it; drop the Group option", streams.ErrUnsupported, consumer)
	}

	attach := func() (attachment, error) { return m.attach(ctx, subject, safeName(consumer), false) }
	a, err := attach()
	if err != nil {
		return nil, err
	}

	m.store.log.Info(ctx, "consuming", telemetry.Fields{"subject": subject, "consumer": consumer})

	out := make(chan streams.Delivery, core.Prefetch(o))
	go func() {
		defer close(out)
		m.follow(ctx, subject, a, attach, func(d amqp.Delivery) bool {
			msg, derr := core.Unpack(m.store.registry, d.RoutingKey, d.Body)
			if derr != nil {
				// Nothing will ever decode this, so no handler will ever
				// acknowledge it. Reject it without requeueing, which is the
				// one case where discarding is right: requeueing would hand the
				// same undecodable bytes to the next consumer forever.
				m.store.log.Warn(ctx, "rejecting a malformed message", telemetry.Fields{"subject": subject, "error": derr.Error()})
				_ = d.Reject(false)
				return true
			}
			m.store.metrics.Delivered(ctx, subject, consumer)
			ack := &amqpAck{del: d, metrics: m.store.metrics, subject: subject, consumer: consumer}
			select {
			case out <- streams.NewDelivery(msg, attempt(&d), ack):
				return true
			case <-ctx.Done():
				return false
			}
		})
	}()
	return out, nil
}

// attachment is one channel consuming one queue.
type attachment struct {
	ch         *amqp.Channel
	deliveries <-chan amqp.Delivery
}

// attach declares a queue, binds it to the subject, and starts consuming on a
// channel of its own — prefetch is per channel, which is why each consumer gets
// one.
//
// A transient queue is exclusive and auto-deleting and belongs to one
// subscription; a durable one is named after the consumer and outlives it.
func (m *manager) attach(ctx context.Context, subject, consumer string, transient bool) (attachment, error) {
	conn, err := m.store.connection(ctx)
	if err != nil {
		return attachment{}, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return attachment{}, fmt.Errorf("rabbitmq: cannot open a channel for %q: %w", subject, err)
	}

	fail := func(what string, err error) (attachment, error) {
		_ = ch.Close()
		return attachment{}, fmt.Errorf("rabbitmq: cannot %s for %q: %w", what, subject, err)
	}
	if m.store.cfg.prefetch > math.MaxUint16 {
		return fail("set prefetch", fmt.Errorf("%d is above AMQP's limit of %d", m.store.cfg.prefetch, math.MaxUint16))
	}
	if qerr := ch.Qos(m.store.cfg.prefetch, 0, false); qerr != nil {
		return fail("set prefetch", qerr)
	}

	var name string
	if !transient {
		name = m.store.queue(m.stream.ID, consumer, safeName(subject))
	}
	q, err := ch.QueueDeclare(name, !transient, transient, transient, false, nil)
	if err != nil {
		return fail("declare a queue", err)
	}
	if berr := ch.QueueBind(q.Name, subject, m.store.exchange(m.stream.ID), false, nil); berr != nil {
		return fail("bind", berr)
	}

	// autoAck is false in both cases: for a durable consumer because
	// acknowledgement belongs to the caller, and for a subscription because
	// with autoAck the broker ignores prefetch and pushes without bound.
	deliveries, err := ch.ConsumeWithContext(ctx, q.Name, "", false, transient, false, false, nil)
	if err != nil {
		return fail("consume", err)
	}
	return attachment{ch: ch, deliveries: deliveries}, nil
}
