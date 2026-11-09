package rabbitmq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// manager publishes to and consumes from one stream.
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
// An AMQP binding key is a pattern, so a stream declaring "user.*" accepts
// "user.created" — matched on AMQP's own wildcards.
func (m *manager) checkSubject(ctx context.Context, subject string) error {
	if declares(m.stream.Subjects, subject) {
		return nil
	}
	m.store.log.Error(ctx, "subject is not declared by this stream", nil, telemetry.Fields{
		"subject": subject, "stream": m.stream.ID, "declared": m.stream.Subjects,
	})
	return core.ErrSubject(m.stream.ID, subject, m.stream.Subjects)
}

// Publish sends a value on a subject, as a routing key on the stream's
// exchange, and returns once the broker has confirmed it.
//
// The message is marked persistent, so the broker writes it to disk rather than
// holding it only in memory. Without that a durable queue would still lose its
// contents to a broker restart, which is most of what "durable" is for.
//
// While the connection is down and being dialed again, Publish waits for it for
// as long as ctx allows — give ctx a deadline to bound that.
func (m *manager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	ids, errs, err := m.send(ctx, subject, []any{value}, streams.NewOptions(opts...))
	if err != nil {
		return "", err
	}
	if errs[0] != nil {
		return "", fmt.Errorf("rabbitmq: cannot publish on %q: %w", subject, errs[0])
	}
	return ids[0], nil
}

// PublishBatch sends several values on a subject and waits once for the broker
// to confirm them all, rather than once per value.
func (m *manager) PublishBatch(ctx context.Context, subject string, values []any, opts ...streams.Option) ([]string, error) {
	o := streams.NewOptions(opts...)
	if err := core.CheckBatch(o); err != nil {
		return nil, err
	}
	ids, errs, err := m.send(ctx, subject, values, o)
	if err != nil {
		return ids, err
	}
	var failures []error
	for i, e := range errs {
		if e != nil {
			failures = append(failures, fmt.Errorf("entry %d: %w", i, e))
		}
	}
	return ids, core.BatchError(subject, len(values), failures)
}

// send encodes values and publishes them, returning their ids and one error
// per value; the last error is for a failure before anything was sent.
func (m *manager) send(ctx context.Context, subject string, values []any, o streams.Options) ([]string, []error, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, nil, err
	}
	if hasWildcard(subject) {
		return nil, nil, fmt.Errorf("%w: cannot publish to the wildcard subject %q, only subscribe to it", streams.ErrUnknownSubject, subject)
	}
	if o.TTL > 0 {
		return nil, nil, fmt.Errorf("%w: this provider publishes immediately; RabbitMQ delays need a dead-letter exchange or the delayed-message plugin", streams.ErrUnsupported)
	}
	if o.Retain {
		return nil, nil, fmt.Errorf("%w: RabbitMQ has no last value per routing key: a queue hands each message to one consumer and then forgets it, and a subscriber that binds later starts empty", streams.ErrUnsupported)
	}
	if err := short("routing key", subject); err != nil {
		return nil, nil, err
	}
	if err := short("message id", o.ID); err != nil {
		return nil, nil, err
	}
	props, err := m.properties(o)
	if err != nil {
		return nil, nil, err
	}

	ids := make([]string, len(values))
	msgs := make([]amqp.Publishing, len(values))
	for i, value := range values {
		ids[i] = o.ID
		if ids[i] == "" {
			ids[i] = core.NewID()
		}
		// Headers travel in the properties, never in the frame, so the body
		// is the version-1 frame whatever the headers.
		body, err := core.Pack(m.store.codec, ids[i], value)
		if err != nil {
			return ids, nil, err
		}
		msg := props
		msg.MessageId, msg.Body = ids[i], body
		msgs[i] = msg
	}

	errs := m.store.publish(ctx, m.store.exchange(m.stream.ID), subject, msgs)
	for i, err := range errs {
		if err != nil {
			m.store.log.Error(ctx, "could not publish", err, telemetry.Fields{"subject": subject, "id": ids[i]})
		}
	}
	m.store.log.Debug(ctx, "published", telemetry.Fields{"subject": subject, "count": len(values)})
	return ids, errs, nil
}

// properties are what every message of one publish goes out with: persistent,
// the stream's and the publish's headers as an AMQP table, and the expiry as
// the expiration property. Headers RabbitMQ would act on rather than carry
// are refused with [streams.ErrInvalidHeader] before anything is sent.
//
// Expires is the per-message expiration rather than a per-queue
// x-message-ttl, which would apply one TTL to every message in the queue.
// RabbitMQ drops an expired message only once it reaches the head of its
// queue, so one behind a message that has not expired is held, and counted in
// the queue's depth, until everything ahead of it is consumed.
func (m *manager) properties(o streams.Options) (amqp.Publishing, error) {
	headers, err := core.Outgoing(m.stream, o)
	if err != nil {
		return amqp.Publishing{}, err
	}
	if herr := checkHeaders(headers); herr != nil {
		return amqp.Publishing{}, herr
	}
	p := amqp.Publishing{DeliveryMode: amqp.Persistent, Headers: table(headers)}
	if o.Expires > 0 {
		if p.Expiration, err = expiration(o.Expires); err != nil {
			return amqp.Publishing{}, err
		}
	}
	return p, nil
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
