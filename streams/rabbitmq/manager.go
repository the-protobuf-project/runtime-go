package rabbitmq

import (
	"context"
	"fmt"
	"github.com/the-protobuf-project/runtime-go/observability"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
)

// manager publishes to and consumes from one stream.
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
// An AMQP binding key is a pattern, so a stream declaring "user.*" accepts
// "user.created" — matched with DeclaresPattern. AMQP's wildcards are `*` for
// one word and `#` for zero or more, which is close enough to NATS's that
// core's matcher reads them correctly for the `*` case; `#` is handled below.
func (m *manager) checkSubject(ctx context.Context, subject string) error {
	if declares(m.stream.Subjects, subject) {
		return nil
	}
	m.store.log.Error(ctx, "subject is not declared by this stream", nil, observability.Fields{
		"subject": subject, "stream": m.stream.ID, "declared": m.stream.Subjects,
	})
	return core.ErrSubject(m.stream.ID, subject, m.stream.Subjects)
}

// Publish sends a value on a subject, as a routing key on the stream's
// exchange.
//
// The message is marked persistent, so the broker writes it to disk rather than
// holding it only in memory. Without that a durable queue would still lose its
// contents to a broker restart, which is most of what "durable" is for.
func (m *manager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return "", err
	}
	if hasWildcard(subject) {
		return "", fmt.Errorf("%w: cannot publish to the wildcard subject %q, only subscribe to it", streams.ErrUnknownSubject, subject)
	}

	o := streams.NewOptions(opts...)
	if o.TTL > 0 {
		return "", fmt.Errorf("%w: this provider publishes immediately; RabbitMQ delays need a dead-letter exchange or the delayed-message plugin", streams.ErrUnsupported)
	}

	id := o.ID
	if id == "" {
		id = core.NewID()
	}

	body, err := core.Pack(m.store.codec, id, value)
	if err != nil {
		return "", err
	}

	m.store.mu.Lock()
	err = m.store.ch.PublishWithContext(ctx, m.store.exchange(m.stream.ID), subject, false, false,
		amqp.Publishing{
			MessageId:    id,
			Body:         body,
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
		})
	m.store.mu.Unlock()

	if err != nil {
		m.store.log.Error(ctx, "could not publish", err, observability.Fields{"subject": subject, "id": id})
		return "", fmt.Errorf("rabbitmq: cannot publish on %q: %w", subject, err)
	}

	m.store.log.Debug(ctx, "published", observability.Fields{"subject": subject, "id": id, "bytes": len(body)})
	return id, nil
}

// Subscribe returns a channel of messages for a subject.
//
// The queue behind it is exclusive and auto-deleting: it belongs to this
// subscription and is removed when the connection drops, so nothing piles up
// for a subscriber that has gone away. That is what makes this the undurable
// half — reach for [manager.Consume] when a message must survive the reader.
func (m *manager) Subscribe(ctx context.Context, subject string, opts ...streams.Option) (<-chan streams.Message, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}

	ch, deliveries, err := m.attach(ctx, subject, "", true)
	if err != nil {
		return nil, err
	}

	m.store.log.Info(ctx, "subscribed", observability.Fields{"subject": subject})

	out := make(chan streams.Message, core.Prefetch(streams.NewOptions(opts...)))
	go func() {
		defer close(out)
		defer func() { _ = ch.Close() }()

		delivered := 0
		defer func() {
			m.store.log.Info(ctx, "subscription closed",
				observability.Fields{"subject": subject, "delivered": delivered})
		}()

		for d := range deliveries {
			msg, derr := core.Unpack(m.store.registry, d.RoutingKey, d.Body)
			if derr != nil {
				m.store.log.Warn(ctx, "dropping a malformed message",
					observability.Fields{"subject": subject, "error": derr.Error()})
				continue
			}
			delivered++
			select {
			case out <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// Consume delivers messages under a named consumer.
//
// The name is a durable queue bound to the stream's exchange for this subject.
// It is the broker that holds the queue, so it outlives the process reading it:
// messages published while nobody is consuming are waiting when the name comes
// back, and a message taken by a consumer that dies unacknowledged is given to
// whoever is next.
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

	ch, deliveries, err := m.attach(ctx, subject, safeName(consumer), false)
	if err != nil {
		return nil, err
	}

	m.store.log.Info(ctx, "consuming", observability.Fields{"subject": subject, "consumer": consumer})

	out := make(chan streams.Delivery, core.Prefetch(o))
	go func() {
		defer close(out)
		defer func() { _ = ch.Close() }()

		delivered := 0
		defer func() {
			m.store.log.Info(ctx, "consumer stopped", observability.Fields{
				"subject": subject, "consumer": consumer, "delivered": delivered,
			})
		}()

		for d := range deliveries {
			msg, derr := core.Unpack(m.store.registry, d.RoutingKey, d.Body)
			if derr != nil {
				// Nothing will ever decode this, so no handler will ever
				// acknowledge it. Reject it without requeueing, which is the
				// one case where discarding is right: requeueing would hand the
				// same undecodable bytes to the next consumer forever.
				m.store.log.Warn(ctx, "rejecting a malformed message",
					observability.Fields{"subject": subject, "error": derr.Error()})
				_ = d.Reject(false)
				continue
			}

			del := d // the loop variable is reused for the next delivery
			delivered++
			m.store.metrics.Delivered(ctx, subject, consumer)
			select {
			case out <- streams.NewDelivery(msg, attempt(&del), &amqpAck{
				del: del, metrics: m.store.metrics, subject: subject, consumer: consumer,
			}):
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// attach declares a queue, binds it to the subject, and starts consuming.
//
// A transient queue is exclusive and auto-deleting and belongs to one
// subscription; a durable one is named after the consumer and outlives it.
func (m *manager) attach(ctx context.Context, subject, consumer string, transient bool) (*amqp.Channel, <-chan amqp.Delivery, error) {
	ch, err := m.store.conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("rabbitmq: cannot open a channel for %q: %w", subject, err)
	}

	// Prefetch is per channel, which is why each consumer gets its own.
	if qerr := ch.Qos(m.store.cfg.prefetch, 0, false); qerr != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("rabbitmq: cannot set prefetch for %q: %w", subject, qerr)
	}

	var name string
	if !transient {
		name = m.store.queue(m.stream.ID, consumer, safeName(subject))
	}

	q, err := ch.QueueDeclare(name, !transient, transient, transient, false, nil)
	if err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("rabbitmq: cannot declare a queue for %q: %w", subject, err)
	}
	if berr := ch.QueueBind(q.Name, subject, m.store.exchange(m.stream.ID), false, nil); berr != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("rabbitmq: cannot bind %q: %w", subject, berr)
	}

	// autoAck is false in both cases: for a durable consumer because
	// acknowledgement belongs to the caller, and for a subscription because
	// the channel closing is what ends it either way.
	deliveries, cerr := ch.ConsumeWithContext(ctx, q.Name, "", transient, transient, false, false, nil)
	if cerr != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("rabbitmq: cannot consume %q: %w", subject, cerr)
	}
	return ch, deliveries, nil
}

// attempt reports how many times a delivery has been tried.
//
// Quorum queues count redeliveries in an x-delivery-count header; classic
// queues carry only a redelivered flag, which says a message has been seen
// before but not how often. The flag is worth reporting as "at least the
// second" rather than discarding — 0 is reserved for knowing nothing at all.
func attempt(d *amqp.Delivery) int {
	if raw, ok := d.Headers["x-delivery-count"]; ok {
		switch n := raw.(type) {
		case int64:
			return int(n) + 1
		case int32:
			return int(n) + 1
		case int:
			return n + 1
		}
	}
	if d.Redelivered {
		return 2
	}
	return 1
}

// declares reports whether any declared subject matches, on AMQP's wildcards.
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
// AMQP publishes are already asynchronous on the channel, so publishing in turn
// does not wait per message.
func (m *manager) PublishBatch(ctx context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	if err := core.CheckBatch(streams.NewOptions(opts...)); err != nil {
		return nil, err
	}
	return core.PublishEach(ctx, m, subject, values, opts...)
}

// amqpAck settles one delivery. RabbitMQ is the only provider here with a true
// negative acknowledgement, so Nak requeues immediately rather than waiting out
// a visibility timeout.
type amqpAck struct {
	del               amqp.Delivery
	metrics           *core.Metrics
	subject, consumer string
}

func (a *amqpAck) Ack(ctx context.Context) error {
	if err := a.del.Ack(false); err != nil {
		return fmt.Errorf("rabbitmq: cannot acknowledge on %q: %w", a.subject, err)
	}
	a.metrics.Settled(ctx, a.subject, a.consumer, "ack")
	return nil
}

func (a *amqpAck) Nak(ctx context.Context) error {
	// requeue, so it goes back for another consumer rather than being discarded.
	if err := a.del.Nack(false, true); err != nil {
		return fmt.Errorf("rabbitmq: cannot return the message on %q: %w", a.subject, err)
	}
	a.metrics.Settled(ctx, a.subject, a.consumer, "nak")
	return nil
}
