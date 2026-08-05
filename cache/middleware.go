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
//	c = cache.Chain(c,
//	    cache.WithRetryMiddleware(3, 100*time.Millisecond),
//	    cache.WithLoggingMiddleware(log),
//	    cache.WithTelemetryMiddleware(meter),
//	)
//
// The outermost wrapper runs first, so the order above times the retries rather
// than each individual attempt.
type Middleware func(Cache) Cache

// Chain applies middlewares to a Cache, outermost last.
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
		total: m.UpDownCounter("cache_entries", telemetry.WithUnit("1")),
	}
}

// WithTelemetryMiddleware is [WithTelemetry] as a [Middleware], for use with
// [Chain].
func WithTelemetryMiddleware(m telemetry.Meter) Middleware {
	return func(c Cache) Cache { return WithTelemetry(c, m) }
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
// idempotent here (Create can mint an id, Update overwrites), and replaying a
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
func (r *retryCache) Create(ctx context.Context, id string, value any, opts ...Option) (string, error) {
	return r.next.Create(ctx, id, value, opts...)
}

// Update is not retried; see [WithRetry].
func (r *retryCache) Update(ctx context.Context, id string, value any, opts ...Option) error {
	return r.next.Update(ctx, id, value, opts...)
}

func (r *retryCache) Get(ctx context.Context, id string, dest any) error {
	return r.retry(ctx, func() error { return r.next.Get(ctx, id, dest) })
}

func (r *retryCache) Delete(ctx context.Context, id string) error {
	return r.retry(ctx, func() error { return r.next.Delete(ctx, id) })
}

func (r *retryCache) Keys(ctx context.Context) ([]string, error) {
	var keys []string
	err := r.retry(ctx, func() error {
		var err error
		keys, err = r.next.Keys(ctx)
		return err
	})
	return keys, err
}

func (r *retryCache) List(ctx context.Context, dest any) error {
	return r.retry(ctx, func() error { return r.next.List(ctx, dest) })
}

func (r *retryCache) TTL(ctx context.Context, id string) (time.Duration, error) {
	var ttl time.Duration
	err := r.retry(ctx, func() error {
		var err error
		ttl, err = r.next.TTL(ctx, id)
		return err
	})
	return ttl, err
}
