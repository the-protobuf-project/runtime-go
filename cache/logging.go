package cache

import (
	"context"
	"errors"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// loggingCache records every operation.
type loggingCache struct {
	next Cache
	log  telemetry.Logger
}

// WithLogging logs each operation: a debug record on success, an error record
// on failure, and a warn record for a miss on a read.
//
// The logger is injected rather than resolved from a package-level global, so a
// binary that never wires logging pays nothing and no import can start writing
// to stderr behind the caller's back. Pass [telemetry.NoopLogger] to disable
// logging without unwrapping.
//
// Bind a component name once and it appears on every record:
//
//	c = cache.WithLogging(c, log.With(telemetry.Fields{"component": "cache"}))
func WithLogging(next Cache, log telemetry.Logger) Cache {
	if log == nil {
		log = telemetry.NoopLogger
	}
	return &loggingCache{next: next, log: log}
}

// WithLoggingMiddleware is [WithLogging] as a [Middleware], for use with
// [Chain].
func WithLoggingMiddleware(log telemetry.Logger) Middleware {
	return func(c Cache) Cache { return WithLogging(c, log) }
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

func (l *loggingCache) Create(ctx context.Context, doc Document) (*Document, error) {
	start := time.Now()
	out, err := l.next.Create(ctx, doc)

	id := doc.ID()
	if out != nil {
		// Create assigns an ID when the caller did not supply one; log the ID
		// that was actually stored.
		id = out.ID()
	}
	l.record(ctx, "create", id, start, err)
	return out, err
}

func (l *loggingCache) Get(ctx context.Context, id string) (Document, error) {
	start := time.Now()
	doc, err := l.next.Get(ctx, id)
	l.record(ctx, "get", id, start, err)
	return doc, err
}

func (l *loggingCache) Update(ctx context.Context, id string, doc Document) error {
	start := time.Now()
	err := l.next.Update(ctx, id, doc)
	l.record(ctx, "update", id, start, err)
	return err
}

func (l *loggingCache) Delete(ctx context.Context, id string) error {
	start := time.Now()
	err := l.next.Delete(ctx, id)
	l.record(ctx, "delete", id, start, err)
	return err
}

func (l *loggingCache) List(ctx context.Context) ([]Document, error) {
	start := time.Now()
	docs, err := l.next.List(ctx)
	l.record(ctx, "list", "", start, err)

	// The count is the useful part of a list, and only worth assembling when
	// something is listening.
	if err == nil && l.log.Enabled(ctx, telemetry.LevelDebug) {
		l.log.Debug(ctx, "cache list returned", telemetry.Fields{"count": len(docs)})
	}
	return docs, err
}
