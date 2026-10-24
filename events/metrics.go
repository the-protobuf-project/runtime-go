package events

import (
	"context"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Attempt outcomes, the attempts counter's "outcome" label.
const (
	OutcomeSucceeded = "succeeded"
	OutcomeRetrying  = "retrying"
	OutcomeFailed    = "failed"
)

// deliveryMetrics is what delivery reports: every attempt by how it ended
// (<prefix>.attempts{outcome}), and for each delivery that succeeded, how long
// after it was recorded -- close to the change itself -- the endpoint accepted
// it (<prefix>.delivery_lag).
type deliveryMetrics struct {
	attempts telemetry.Counter
	lag      telemetry.Histogram
}

func newDeliveryMetrics(m telemetry.Meter, prefix string) deliveryMetrics {
	if m == nil {
		m = telemetry.NoopMeter
	}
	return deliveryMetrics{
		attempts: m.Counter(prefix+".attempts", telemetry.WithUnit("1"),
			telemetry.WithDescription("Webhook delivery attempts, by outcome: succeeded, retrying (failed, will retry) or failed (gave up).")),
		lag: m.Histogram(prefix+".delivery_lag", telemetry.WithUnit("s"),
			telemetry.WithDescription("From a delivery being recorded to its endpoint accepting it."),
			telemetry.WithBuckets(0.5, 1, 2, 5, 10, 30, 60, 300, 1800, 3600, 21600, 86400)),
	}
}

func (m deliveryMetrics) attempt(ctx context.Context, outcome string) {
	m.attempts.Add(ctx, 1, telemetry.Labels{"outcome": outcome})
}

func (m deliveryMetrics) delivered(ctx context.Context, lag time.Duration) {
	m.lag.Record(ctx, lag.Seconds(), telemetry.Labels{})
}
