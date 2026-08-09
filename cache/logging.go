package cache

import (
	"context"
	"errors"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// loggingCache records every operation.
type loggingCache struct {
	next Document
	log  telemetry.Logger
}

// WithLogging logs each operation: a debug record on success, an error record
// on failure, and a warn record for a miss on a read.
//
// This is the uniform per-call record. Providers log their own internals —
// which key an id resolved to, which stale entries were swept — through the
// logger given to their config; the two compose.
//
// The logger is injected rather than resolved from a package-level global, so a
// binary that never wires logging pays nothing and no import can start writing
// to stderr behind the caller's back. Pass [telemetry.NoopLogger] to disable
// logging without unwrapping.
func WithLogging(next Document, log telemetry.Logger) Document {
	if log == nil {
		log = telemetry.NoopLogger
	}
	return &loggingCache{next: next, log: log}
}

// WithLoggingMiddleware is [WithLogging] as a [Middleware], for use with
// [Chain].
func WithLoggingMiddleware(log telemetry.Logger) Middleware {
	return func(c Document) Document { return WithLogging(c, log) }
}

// record writes the outcome of one operation.
//
// A miss is logged at warn rather than error: an absent key is a normal cache
// result, and logging it as a failure would bury real errors in noise. The
// duration is always included — a slow cache is the usual thing you are looking
// for when you turn these on.
func (l *loggingCache) record(ctx context.Context, op, id string, start time.Time, err error) {
	fields := telemetry.Fields{
		"operation": op,
		"duration":  time.Since(start).String(),
	}
	if id != "" {
		fields["id"] = id
	}

	switch {
	case err == nil:
		l.log.Debug(ctx, "cache "+op, fields)
	case errors.Is(err, ErrNotFound):
		l.log.Warn(ctx, "cache miss", fields)
	default:
		l.log.Error(ctx, "cache "+op+" failed", err, fields)
	}
}

func (l *loggingCache) Create(ctx context.Context, id string, value any, opts ...Option) (string, error) {
	start := time.Now()
	out, err := l.next.Create(ctx, id, value, opts...)

	// Create mints an id when the caller supplies none; log the one that was
	// actually used.
	logged := id
	if out != "" {
		logged = out
	}
	l.record(ctx, "create", logged, start, err)
	return out, err
}

func (l *loggingCache) Get(ctx context.Context, id string, dest any) error {
	start := time.Now()
	err := l.next.Get(ctx, id, dest)
	l.record(ctx, "get", id, start, err)
	return err
}

func (l *loggingCache) Update(ctx context.Context, id string, value any, opts ...Option) error {
	start := time.Now()
	err := l.next.Update(ctx, id, value, opts...)
	l.record(ctx, "update", id, start, err)
	return err
}

func (l *loggingCache) Delete(ctx context.Context, id string) error {
	start := time.Now()
	err := l.next.Delete(ctx, id)
	l.record(ctx, "delete", id, start, err)
	return err
}

func (l *loggingCache) Keys(ctx context.Context) ([]string, error) {
	start := time.Now()
	keys, err := l.next.Keys(ctx)
	l.record(ctx, "keys", "", start, err)

	if err == nil && l.log.Enabled(ctx, telemetry.LevelDebug) {
		l.log.Debug(ctx, "cache keys returned", telemetry.Fields{"count": len(keys)})
	}
	return keys, err
}

func (l *loggingCache) List(ctx context.Context, dest any) error {
	start := time.Now()
	err := l.next.List(ctx, dest)
	l.record(ctx, "list", "", start, err)
	return err
}

func (l *loggingCache) TTL(ctx context.Context, id string) (time.Duration, error) {
	start := time.Now()
	ttl, err := l.next.TTL(ctx, id)
	l.record(ctx, "ttl", id, start, err)
	return ttl, err
}
