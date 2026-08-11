package core

import (
	"context"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Metrics is what every provider reports about delivery.
//
// Consumer lag is the number that matters in production — it is the only one
// that says whether a program is keeping up — and until this existed the layer
// could not report it at all. These are the parts of it a provider can measure
// without asking the broker anything: what it handed over, what came back
// settled, and the difference between them.
type Metrics struct {
	delivered telemetry.Counter
	settled   telemetry.Counter
	inflight  telemetry.UpDownCounter
	lag       telemetry.Gauge
}

// NewMetrics builds the instruments once, so a delivery costs a record rather
// than a lookup. A nil meter is the caller not wanting any of this, and
// [telemetry.NoopMeter] makes that free rather than conditional.
func NewMetrics(m telemetry.Meter) *Metrics {
	if m == nil {
		m = telemetry.NoopMeter
	}
	return &Metrics{
		delivered: m.Counter("streams_delivered_total", telemetry.WithUnit("1")),
		settled:   m.Counter("streams_settled_total", telemetry.WithUnit("1")),
		inflight:  m.UpDownCounter("streams_inflight", telemetry.WithUnit("1")),
		lag:       m.Gauge("streams_consumer_lag", telemetry.WithUnit("1")),
	}
}

// Delivered records a message handed to a consumer, and counts it as
// outstanding until it is settled.
func (x *Metrics) Delivered(ctx context.Context, subject, consumer string) {
	if x == nil {
		return
	}
	labels := telemetry.Labels{"subject": subject, "consumer": consumer}
	x.delivered.Add(ctx, 1, labels)
	x.inflight.Add(ctx, 1, labels)
}

// Settled records a delivery acknowledged or returned, and clears it from the
// outstanding count.
//
// outcome is "ack" or "nak". They share one counter rather than having one
// each, so the ratio between them is a query rather than an arithmetic
// operation on two series — and a rising nak rate is what a poison message
// looks like from the outside.
func (x *Metrics) Settled(ctx context.Context, subject, consumer, outcome string) {
	if x == nil {
		return
	}
	x.settled.Add(ctx, 1, telemetry.Labels{
		"subject": subject, "consumer": consumer, "outcome": outcome,
	})
	x.inflight.Add(ctx, -1, telemetry.Labels{"subject": subject, "consumer": consumer})
}

// Lag records how far behind a named consumer is, in messages.
//
// Only providers whose backend can answer that question report it: Kafka from
// its offsets, Redis from its pending list, JetStream from its consumer info.
// Where the backend cannot say, the series is absent rather than zero — a
// consumer that looks perfectly caught up because nothing can measure it is
// worse than one that does not appear at all.
func (x *Metrics) Lag(ctx context.Context, subject, consumer string, n int64) {
	if x == nil {
		return
	}
	x.lag.Set(ctx, float64(n), telemetry.Labels{"subject": subject, "consumer": consumer})
}
