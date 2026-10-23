package cache

import (
	"context"
	"errors"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// WithVolatileTelemetry is [WithTelemetry] for a [Volatile]: an operation
// count and duration histogram for every call, and hit/miss for every Get,
// on the same instruments, labeled strategy=volatile. A list cache reading
// pages through Volatile reports its hit rate this way.
//
// A miss is an outcome, not an error, as in [WithTelemetry].
func WithVolatileTelemetry(next Volatile, m telemetry.Meter) Volatile {
	if m == nil {
		m = telemetry.NoopMeter
	}
	return &telemetryVolatile{
		next: next,
		ops:  m.Counter("cache_operations_total", telemetry.WithUnit("1")),
		dur:  m.Histogram("cache_operation_duration_seconds", telemetry.WithUnit("s")),
		hits: m.Counter("cache_gets_total", telemetry.WithUnit("1")),
	}
}

type telemetryVolatile struct {
	next Volatile
	ops  telemetry.Counter
	dur  telemetry.Histogram
	hits telemetry.Counter
}

func (t *telemetryVolatile) record(ctx context.Context, op string, start time.Time, err error) {
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	labels := telemetry.Labels{"operation": op, "outcome": outcome, "strategy": "volatile"}
	t.ops.Add(ctx, 1, labels)
	t.dur.Record(ctx, time.Since(start).Seconds(), labels)
}

func (t *telemetryVolatile) Set(ctx context.Context, key string, value any, opts ...Option) error {
	start := time.Now()
	err := t.next.Set(ctx, key, value, opts...)
	t.record(ctx, "set", start, err)
	return err
}

func (t *telemetryVolatile) Get(ctx context.Context, key string, dest any) error {
	start := time.Now()
	err := t.next.Get(ctx, key, dest)
	switch {
	case errors.Is(err, ErrNotFound):
		t.hits.Add(ctx, 1, telemetry.Labels{"result": "miss", "strategy": "volatile"})
		t.record(ctx, "get", start, nil)
	case err == nil:
		t.hits.Add(ctx, 1, telemetry.Labels{"result": "hit", "strategy": "volatile"})
		t.record(ctx, "get", start, nil)
	default:
		t.record(ctx, "get", start, err)
	}
	return err
}

func (t *telemetryVolatile) Delete(ctx context.Context, key string) error {
	start := time.Now()
	err := t.next.Delete(ctx, key)
	t.record(ctx, "delete", start, err)
	return err
}

func (t *telemetryVolatile) Touch(ctx context.Context, key string, ttl time.Duration) error {
	start := time.Now()
	err := t.next.Touch(ctx, key, ttl)
	t.record(ctx, "touch", start, err)
	return err
}

func (t *telemetryVolatile) TTL(ctx context.Context, key string) (time.Duration, error) {
	start := time.Now()
	ttl, err := t.next.TTL(ctx, key)
	t.record(ctx, "ttl", start, err)
	return ttl, err
}

func (t *telemetryVolatile) Scan(ctx context.Context, pattern string) ([]string, error) {
	start := time.Now()
	keys, err := t.next.Scan(ctx, pattern)
	t.record(ctx, "scan", start, err)
	return keys, err
}
