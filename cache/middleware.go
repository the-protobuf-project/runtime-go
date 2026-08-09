package cache

import (
	"context"
	"errors"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Middleware wraps a Document in behavior that applies to any provider —
// instrumentation, retries, rate limiting. Because it takes and returns the
// same interface, middlewares compose:
//
//	c = cache.Chain(c,
//	    cache.WithRetryMiddleware(3, 100*time.Millisecond),
//	    cache.WithLoggingMiddleware(log),
//	    cache.WithTelemetryMiddleware(meter),
//	)
//
// The outermost wrapper runs first, so the order above times the retries rather
// than each individual attempt.
type Middleware func(Document) Document

// Chain applies middlewares to a Document, outermost last.
func Chain(c Document, mw ...Middleware) Document {
	for _, m := range mw {
		c = m(c)
	}
	return c
}

// telemetryCache instruments every operation with a counter and a duration
// histogram.
type telemetryCache struct {
	next  Document
	ops   telemetry.Counter
	dur   telemetry.Histogram
	hits  telemetry.Counter
	total telemetry.UpDownCounter
}

// WithTelemetry records an operation count, a duration histogram, and cache
// hit/miss for reads.
//
// The meter is injected rather than resolved from a package-level global, so a
// binary that never wires telemetry pays nothing and no import can start a
// background exporter behind the caller's back. Pass [telemetry.NoopMeter] to
// disable instrumentation without unwrapping.
//
// A miss is recorded as a hit/miss outcome, not an error: an absent key is a
// normal cache result, and counting it as a failure makes error rates useless.
func WithTelemetry(next Document, m telemetry.Meter) Document {
	if m == nil {
		m = telemetry.NoopMeter
	}
	return &telemetryCache{
		next: next,
		ops:  m.Counter("cache_operations_total", telemetry.WithUnit("1")),
		dur: m.Histogram("cache_operation_duration_seconds",
			telemetry.WithUnit("s")),
		hits:  m.Counter("cache_gets_total", telemetry.WithUnit("1")),
		total: m.UpDownCounter("cache_entries", telemetry.WithUnit("1")),
	}
}

// WithTelemetryMiddleware is [WithTelemetry] as a [Middleware], for use with
// [Chain].
func WithTelemetryMiddleware(m telemetry.Meter) Middleware {
	return func(c Document) Document { return WithTelemetry(c, m) }
}

// record reports one completed operation. outcome is "ok" or "error" so a
// dashboard can compute an error rate from a single series.
func (t *telemetryCache) record(ctx context.Context, op string, start time.Time, err error) {
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	labels := telemetry.Labels{"operation": op, "outcome": outcome}
	t.ops.Add(ctx, 1, labels)
	t.dur.Record(ctx, time.Since(start).Seconds(), labels)
}

func (t *telemetryCache) Create(ctx context.Context, id string, value any, opts ...Option) (string, error) {
	start := time.Now()
	out, err := t.next.Create(ctx, id, value, opts...)
	t.record(ctx, "create", start, err)
	if err == nil {
		t.total.Add(ctx, 1, telemetry.Labels{})
	}
	return out, err
}

func (t *telemetryCache) Get(ctx context.Context, id string, dest any) error {
	start := time.Now()
	err := t.next.Get(ctx, id, dest)

	// A miss is an expected outcome, not a failure — report it on the hit/miss
	// series and keep it out of the error count.
	if errors.Is(err, ErrNotFound) {
		t.hits.Add(ctx, 1, telemetry.Labels{"result": "miss"})
		t.record(ctx, "get", start, nil)
		return err
	}
	if err == nil {
		t.hits.Add(ctx, 1, telemetry.Labels{"result": "hit"})
	}
	t.record(ctx, "get", start, err)
	return err
}

func (t *telemetryCache) Update(ctx context.Context, id string, value any, opts ...Option) error {
	start := time.Now()
	err := t.next.Update(ctx, id, value, opts...)
	t.record(ctx, "update", start, err)
	return err
}

func (t *telemetryCache) Delete(ctx context.Context, id string) error {
	start := time.Now()
	err := t.next.Delete(ctx, id)
	t.record(ctx, "delete", start, err)
	if err == nil {
		t.total.Add(ctx, -1, telemetry.Labels{})
	}
	return err
}

func (t *telemetryCache) Keys(ctx context.Context) ([]string, error) {
	start := time.Now()
	keys, err := t.next.Keys(ctx)
	t.record(ctx, "keys", start, err)
	return keys, err
}

func (t *telemetryCache) List(ctx context.Context, dest any) error {
	start := time.Now()
	err := t.next.List(ctx, dest)
	t.record(ctx, "list", start, err)
	return err
}

func (t *telemetryCache) TTL(ctx context.Context, id string) (time.Duration, error) {
	start := time.Now()
	ttl, err := t.next.TTL(ctx, id)
	t.record(ctx, "ttl", start, err)
	return ttl, err
}
