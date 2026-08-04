package database

import (
	"context"
	"errors"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Middleware wraps a Store in behavior that applies to any provider —
// instrumentation, retries, rate limiting. Because it takes and returns the
// same interface, middlewares compose:
//
//	db = database.WithRetry(db, 3, 100*time.Millisecond)
//	db = database.WithTelemetry(db, meter)
//
// The outermost wrapper runs first, so the order above times the retries rather
// than each individual attempt. Swap the two lines to measure attempts instead.
type Middleware func(Store) Store

// Chain applies middlewares to a Store, outermost last.
func Chain(s Store, mw ...Middleware) Store {
	for _, m := range mw {
		s = m(s)
	}
	return s
}

// telemetryStore instruments every operation with a counter and a duration
// histogram.
type telemetryStore struct {
	next  Store
	ops   telemetry.Counter
	dur   telemetry.Histogram
	total telemetry.UpDownCounter
}

// WithTelemetry records an operation count and a duration histogram.
//
// The meter is injected rather than resolved from a package-level global, so a
// binary that never wires telemetry pays nothing and no import can start a
// background exporter behind the caller's back. Pass [telemetry.NoopMeter] to
// disable instrumentation without unwrapping.
//
// Unlike a cache, [ErrNotFound] here counts as an error: a missing record is a
// genuine failure rather than a routine outcome.
func WithTelemetry(next Store, m telemetry.Meter) Store {
	if m == nil {
		m = telemetry.NoopMeter
	}
	return &telemetryStore{
		next: next,
		ops:  m.Counter("database_operations_total", telemetry.WithUnit("1")),
		dur: m.Histogram("database_operation_duration_seconds",
			telemetry.WithUnit("s")),
		total: m.UpDownCounter("database_documents", telemetry.WithUnit("1")),
	}
}

func (t *telemetryStore) record(ctx context.Context, op string, start time.Time, err error) {
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	labels := telemetry.Labels{"operation": op, "outcome": outcome}
	t.ops.Add(ctx, 1, labels)
	t.dur.Record(ctx, time.Since(start).Seconds(), labels)
}

func (t *telemetryStore) Create(ctx context.Context, doc Document) (*Document, error) {
	start := time.Now()
	out, err := t.next.Create(ctx, doc)
	t.record(ctx, "create", start, err)
	if err == nil {
		t.total.Add(ctx, 1, telemetry.Labels{})
	}
	return out, err
}

func (t *telemetryStore) Get(ctx context.Context, id string) (Document, error) {
	start := time.Now()
	doc, err := t.next.Get(ctx, id)
	t.record(ctx, "get", start, err)
	return doc, err
}

func (t *telemetryStore) Update(ctx context.Context, id string, doc Document) error {
	start := time.Now()
	err := t.next.Update(ctx, id, doc)
	t.record(ctx, "update", start, err)
	return err
}

func (t *telemetryStore) Delete(ctx context.Context, id string) error {
	start := time.Now()
	err := t.next.Delete(ctx, id)
	t.record(ctx, "delete", start, err)
	if err == nil {
		t.total.Add(ctx, -1, telemetry.Labels{})
	}
	return err
}

func (t *telemetryStore) List(ctx context.Context, q Query) ([]Document, error) {
	start := time.Now()
	docs, err := t.next.List(ctx, q)
	t.record(ctx, "list", start, err)
	return docs, err
}

// retryStore retries operations that failed for a reason a retry could fix.
type retryStore struct {
	next     Store
	attempts int
	backoff  time.Duration
}

// WithRetry retries failed operations with exponential backoff, up to attempts
// total tries (so attempts=3 means one try and two retries). A non-positive
// attempts count disables retrying and returns next unchanged.
//
// Reads are retried; writes are not — Create and Update are not idempotent
// here, and replaying a half-applied write can store a second copy rather than
// repair the first. Retrying [ErrNotFound] or [ErrDuplicate] is likewise
// pointless: both are settled answers that a retry will not change.
//
// Backoff respects the context: a canceled or expired ctx stops the retry loop
// immediately rather than sleeping out the remaining attempts.
func WithRetry(next Store, attempts int, backoff time.Duration) Store {
	if attempts <= 1 {
		return next
	}
	return &retryStore{next: next, attempts: attempts, backoff: backoff}
}

// WithRetryMiddleware is [WithRetry] as a [Middleware], for use with [Chain].
func WithRetryMiddleware(attempts int, backoff time.Duration) Middleware {
	return func(s Store) Store { return WithRetry(s, attempts, backoff) }
}

// WithTelemetryMiddleware is [WithTelemetry] as a [Middleware], for use with
// [Chain].
func WithTelemetryMiddleware(m telemetry.Meter) Middleware {
	return func(s Store) Store { return WithTelemetry(s, m) }
}

// retry runs op until it succeeds, the context ends, or the attempts run out.
func (r *retryStore) retry(ctx context.Context, op func() error) error {
	var err error
	wait := r.backoff

	for i := range r.attempts {
		if err = op(); err == nil {
			return nil
		}
		// Settled answers; retrying only wastes the caller's deadline.
		if errors.Is(err, ErrNotFound) || errors.Is(err, ErrDuplicate) {
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
func (r *retryStore) Create(ctx context.Context, doc Document) (*Document, error) {
	return r.next.Create(ctx, doc)
}

// Update is not retried; see [WithRetry].
func (r *retryStore) Update(ctx context.Context, id string, doc Document) error {
	return r.next.Update(ctx, id, doc)
}

func (r *retryStore) Get(ctx context.Context, id string) (Document, error) {
	var doc Document
	err := r.retry(ctx, func() error {
		var err error
		doc, err = r.next.Get(ctx, id)
		return err
	})
	return doc, err
}

func (r *retryStore) Delete(ctx context.Context, id string) error {
	return r.retry(ctx, func() error { return r.next.Delete(ctx, id) })
}

func (r *retryStore) List(ctx context.Context, q Query) ([]Document, error) {
	var docs []Document
	err := r.retry(ctx, func() error {
		var err error
		docs, err = r.next.List(ctx, q)
		return err
	})
	return docs, err
}
