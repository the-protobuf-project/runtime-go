package streams

import (
	"context"
	"testing"
)

// The shape this used to be: two closures captured per delivery.
type oldDelivery struct {
	Message
	Attempt int
	Ack     func(ctx context.Context) error
	Nak     func(ctx context.Context) error
}

// handle stands in for whatever a provider must capture to settle a message —
// a Kafka record, a JetStream message, an AMQP delivery.
type handle struct{ id string }

func (h *handle) Ack(context.Context) error { _ = h.id; return nil }
func (h *handle) Nak(context.Context) error { _ = h.id; return nil }

var (
	sinkOld oldDelivery
	sinkNew Delivery
)

// What Phase 4 changed: settling a delivery no longer costs a closure per
// method on a path that runs once per message.
func BenchmarkDeliveryShape(b *testing.B) {
	h := &handle{id: "x"}
	_ = h

	b.Run("closures", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			sinkOld = oldDelivery{
				Message: Message{ID: "1"},
				Attempt: 1,
				Ack:     func(context.Context) error { _ = h.id; return nil },
				Nak:     func(context.Context) error { _ = h.id; return nil },
			}
		}
	})

	// The acker is built per delivery, as a provider must: what settles a
	// message is that message's own handle. Reusing one here would measure
	// something no provider can do.
	b.Run("acker", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			sinkNew = NewDelivery(Message{ID: "1"}, 1, &handle{id: "x"})
		}
	})
}
