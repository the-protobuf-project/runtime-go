package streams

import (
	"context"
	"errors"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// PublisherMiddleware wraps a Publisher in behavior that applies to any
// provider — instrumentation, retries, rate limiting.
type PublisherMiddleware func(Publisher) Publisher

// ChainPublisher applies middlewares to a Publisher, outermost last.
func ChainPublisher(p Publisher, mw ...PublisherMiddleware) Publisher {
	for _, m := range mw {
		p = m(p)
	}
	return p
}

// telemetryPublisher counts and times publishes.
type telemetryPublisher struct {
	next Publisher
	ops  telemetry.Counter
	dur  telemetry.Histogram
}

// WithPublisherTelemetry records a publish count and duration histogram,
// labeled by subject.
//
// The meter is injected rather than resolved from a package-level global, so a
// binary that never wires telemetry pays nothing and no import can start a
// background exporter behind the caller's back. Pass [telemetry.NoopMeter] to
// disable instrumentation without unwrapping.
func WithPublisherTelemetry(next Publisher, m telemetry.Meter) Publisher {
	if m == nil {
		m = telemetry.NoopMeter
	}
	return &telemetryPublisher{
		next: next,
		ops:  m.Counter("streams_published_total", telemetry.WithUnit("1")),
		dur: m.Histogram("streams_publish_duration_seconds",
			telemetry.WithUnit("s")),
	}
}

func (t *telemetryPublisher) Publish(ctx context.Context, subject string, value any, opts ...Option) (string, error) {
	start := time.Now()
	id, err := t.next.Publish(ctx, subject, value, opts...)

	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	labels := telemetry.Labels{"subject": subject, "outcome": outcome}
	t.ops.Add(ctx, 1, labels)
	t.dur.Record(ctx, time.Since(start).Seconds(), labels)
	return id, err
}

// retryPublisher retries publishes that failed for a reason a retry could fix.
type retryPublisher struct {
	next     Publisher
	attempts int
	backoff  time.Duration
}

// WithPublisherRetry retries failed publishes with exponential backoff, up to
// attempts total tries. A non-positive attempts count disables retrying and
// returns next unchanged.
//
// Retrying is safe here in a way it is not for a store write: a redelivered
// message is a duplicate the consumer can dedupe, whereas a dropped one is
// simply lost. An [ErrUnknownSubject] is never retried — the subject will not
// become valid on a second attempt.
//
// Backoff respects the context: a canceled or expired ctx stops the retry loop
// immediately rather than sleeping out the remaining attempts.
func WithPublisherRetry(next Publisher, attempts int, backoff time.Duration) Publisher {
	if attempts <= 1 {
		return next
	}
	return &retryPublisher{next: next, attempts: attempts, backoff: backoff}
}

// WithPublisherRetryMiddleware is [WithPublisherRetry] as a middleware.
func WithPublisherRetryMiddleware(attempts int, backoff time.Duration) PublisherMiddleware {
	return func(p Publisher) Publisher { return WithPublisherRetry(p, attempts, backoff) }
}

// WithPublisherTelemetryMiddleware is [WithPublisherTelemetry] as a middleware.
func WithPublisherTelemetryMiddleware(m telemetry.Meter) PublisherMiddleware {
	return func(p Publisher) Publisher { return WithPublisherTelemetry(p, m) }
}

func (r *retryPublisher) Publish(ctx context.Context, subject string, value any, opts ...Option) (string, error) {
	var (
		err error
		id  string
	)
	wait := r.backoff

	for i := range r.attempts {
		if id, err = r.next.Publish(ctx, subject, value, opts...); err == nil {
			return id, nil
		}
		// A settled answer; retrying only wastes the caller's deadline.
		if errors.Is(err, ErrUnknownSubject) {
			return "", err
		}
		if i == r.attempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return "", errors.Join(err, ctx.Err())
		case <-time.After(wait):
			wait *= 2
		}
	}
	return "", err
}
