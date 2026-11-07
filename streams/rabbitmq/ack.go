package rabbitmq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/the-protobuf-project/runtime-go/streams/core"
)

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
		return fmt.Errorf("rabbitmq: cannot acknowledge on %q; if the connection dropped, the broker delivers it again: %w", a.subject, err)
	}
	a.metrics.Settled(ctx, a.subject, a.consumer, "ack")
	return nil
}

func (a *amqpAck) Nak(ctx context.Context) error {
	// requeue, so it goes back for another consumer rather than being discarded.
	if err := a.del.Nack(false, true); err != nil {
		return fmt.Errorf("rabbitmq: cannot return the message on %q; if the connection dropped, the broker has already: %w", a.subject, err)
	}
	a.metrics.Settled(ctx, a.subject, a.consumer, "nak")
	return nil
}
