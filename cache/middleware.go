package cache

import (
	"context"
	"errors"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Middleware wraps a Cache in behavior that applies to any provider —
// instrumentation, retries, rate limiting. Because it takes and returns the
// same interface, middlewares compose:
//
//	c = cache.WithRetry(c, 3, 100*time.Millisecond)
//	c = cache.WithTelemetry(c, meter)
//
// The outermost wrapper runs first, so the order above times the retries rather
// than each individual attempt. Swap the two lines to measure attempts instead.
type Middleware func(Cache) Cache

// Chain applies middlewares to a Cache, outermost last:
//
//	c = cache.Chain(c, cache.WithRetryMiddleware(3, time.Second), telemetryMW)
//
// is the same as wrapping by hand in that order.
func Chain(c Cache, mw ...Middleware) Cache {
	for _, m := range mw {
		c = m(c)
	}
	return c
}

// telemetryCache instruments every operation with a counter and a duration
// histogram.
type telemetryCache struct {
	next  Cache
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
func WithTelemetry(next Cache, m telemetry.Meter) Cache {
	if m == nil {
		m = telemetry.NoopMeter
	}
	return &telemetryCache{
		next: next,
		ops:  m.Counter("cache_operations_total", telemetry.WithUnit("1")),
		dur: m.Histogram("cache_operation_duration_seconds",
			telemetry.WithUnit("s")),
		hits:  m.Counter("cache_gets_total", telemetry.WithUnit("1")),
		total: m.UpDownCounter("cache_documents", telemetry.WithUnit("1")),
	}
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

func (t *telemetryCache) Create(ctx context.Context, doc Document) (*Document, error) {
	start := time.Now()
	out, err := t.next.Create(ctx, doc)
	t.record(ctx, "create", start, err)
	if err == nil {
		t.total.Add(ctx, 1, telemetry.Labels{})
	}
	return out, err
}

func (t *telemetryCache) Get(ctx context.Context, id string) (Document, error) {
	start := time.Now()
	doc, err := t.next.Get(ctx, id)

	// A miss is an expected outcome, not a failure — report it on the hit/miss
	// series and keep it out of the error count.
	if errors.Is(err, ErrNotFound) {
		t.hits.Add(ctx, 1, telemetry.Labels{"result": "miss"})
		t.record(ctx, "get", start, nil)
		return doc, err
	}
	if err == nil {
		t.hits.Add(ctx, 1, telemetry.Labels{"result": "hit"})
	}
	t.record(ctx, "get", start, err)
	return doc, err
}

func (t *telemetryCache) Update(ctx context.Context, id string, doc Document) error {
	start := time.Now()
	err := t.next.Update(ctx, id, doc)
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

func (t *telemetryCache) List(ctx context.Context) ([]Document, error) {
	start := time.Now()
	docs, err := t.next.List(ctx)
	t.record(ctx, "list", start, err)
	return docs, err
}

// retryCache retries operations that failed for a reason a retry could fix.
type retryCache struct {
	next     Cache
	attempts int
	backoff  time.Duration
}

// WithRetry retries failed operations with exponential backoff, up to attempts
// total tries (so attempts=3 means one try and two retries). A non-positive
// attempts count disables retrying and returns next unchanged.
//
// Reads and deletes are retried; writes are not — Create and Update are not
// idempotent here (Create can mint an ID, Update overwrites), and replaying a
// half-applied write can duplicate an entry rather than repair one. Retrying a
// [ErrNotFound] is likewise pointless: the answer will not change.
//
// Backoff respects the context: a canceled or expired ctx stops the retry loop
// immediately rather than sleeping out the remaining attempts.
func WithRetry(next Cache, attempts int, backoff time.Duration) Cache {
	if attempts <= 1 {
		return next
	}
	return &retryCache{next: next, attempts: attempts, backoff: backoff}
}

// WithRetryMiddleware is [WithRetry] as a [Middleware], for use with [Chain].
func WithRetryMiddleware(attempts int, backoff time.Duration) Middleware {
	return func(c Cache) Cache { return WithRetry(c, attempts, backoff) }
}

// WithTelemetryMiddleware is [WithTelemetry] as a [Middleware], for use with
// [Chain].
func WithTelemetryMiddleware(m telemetry.Meter) Middleware {
	return func(c Cache) Cache { return WithTelemetry(c, m) }
}

// retry runs op until it succeeds, the context ends, or the attempts run out.
// It returns the last error seen.
func (r *retryCache) retry(ctx context.Context, op func() error) error {
	var err error
	wait := r.backoff

	for i := range r.attempts {
		if err = op(); err == nil {
			return nil
		}
		// A missing key is a settled answer; retrying only wastes the caller's
		// deadline.
		if errors.Is(err, ErrNotFound) {
			return err
		}
		if i == r.attempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(wait):
			wait *= 2
		}
	}
	return err
}

// Create is not retried; see [WithRetry].
func (r *retryCache) Create(ctx context.Context, doc Document) (*Document, error) {
	return r.next.Create(ctx, doc)
}

// Update is not retried; see [WithRetry].
func (r *retryCache) Update(ctx context.Context, id string, doc Document) error {
	return r.next.Update(ctx, id, doc)
}

func (r *retryCache) Get(ctx context.Context, id string) (Document, error) {
	var doc Document
	err := r.retry(ctx, func() error {
		var err error
		doc, err = r.next.Get(ctx, id)
		return err
	})
	return doc, err
}

func (r *retryCache) Delete(ctx context.Context, id string) error {
	return r.retry(ctx, func() error { return r.next.Delete(ctx, id) })
}

func (r *retryCache) List(ctx context.Context) ([]Document, error) {
	var docs []Document
	err := r.retry(ctx, func() error {
		var err error
		docs, err = r.next.List(ctx)
		return err
	})
	return docs, err
}
