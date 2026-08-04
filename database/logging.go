package database

import (
	"context"
	"errors"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// loggingStore records every operation.
type loggingStore struct {
	next Store
	log  telemetry.Logger
}

// WithLogging logs each operation: a debug record on success and an error
// record on failure.
//
// The logger is injected rather than resolved from a package-level global, so a
// binary that never wires logging pays nothing. Pass [telemetry.NoopLogger] to
// disable logging without unwrapping.
//
//	db = database.WithLogging(db, log.With(telemetry.Fields{"component": "database"}))
func WithLogging(next Store, log telemetry.Logger) Store {
	if log == nil {
		log = telemetry.NoopLogger
	}
	return &loggingStore{next: next, log: log}
}

// WithLoggingMiddleware is [WithLogging] as a [Middleware], for use with
// [Chain].
func WithLoggingMiddleware(log telemetry.Logger) Middleware {
	return func(s Store) Store { return WithLogging(s, log) }
}

// record writes the outcome of one operation.
//
// Unlike a cache, a missing record is a genuine failure here — documents do not
// expire on their own — so [ErrNotFound] is logged at error. A duplicate is
// logged at warn: the write was refused, but the store is in the state the
// caller wanted, with the content stored exactly once.
func (l *loggingStore) record(ctx context.Context, op, id string, start time.Time, err error) {
	fields := telemetry.Fields{
		"operation": op,
		"duration":  time.Since(start).String(),
	}
	if id != "" {
		fields["id"] = id
	}

	switch {
	case err == nil:
		l.log.Debug(ctx, "database "+op, fields)
	case errors.Is(err, ErrDuplicate):
		l.log.Warn(ctx, "database "+op+" refused: duplicate content", fields)
	default:
		l.log.Error(ctx, "database "+op+" failed", err, fields)
	}
}

func (l *loggingStore) Create(ctx context.Context, doc Document) (*Document, error) {
	start := time.Now()
	out, err := l.next.Create(ctx, doc)

	id := doc.ID()
	if out != nil {
		id = out.ID()
	}
	l.record(ctx, "create", id, start, err)

	// A returned ID that differs from the requested one means the content was
	// already stored — worth surfacing, since the caller's write did not
	// produce a new document.
	if err == nil && out != nil && doc.ID() != "" && out.ID() != doc.ID() {
		l.log.Info(ctx, "database create deduplicated", telemetry.Fields{
			"requested": doc.ID(),
			"existing":  out.ID(),
		})
	}
	return out, err
}

func (l *loggingStore) Get(ctx context.Context, id string) (Document, error) {
	start := time.Now()
	doc, err := l.next.Get(ctx, id)
	l.record(ctx, "get", id, start, err)
	return doc, err
}

func (l *loggingStore) Update(ctx context.Context, id string, doc Document) error {
	start := time.Now()
	err := l.next.Update(ctx, id, doc)
	l.record(ctx, "update", id, start, err)
	return err
}

func (l *loggingStore) Delete(ctx context.Context, id string) error {
	start := time.Now()
	err := l.next.Delete(ctx, id)
	l.record(ctx, "delete", id, start, err)
	return err
}

func (l *loggingStore) List(ctx context.Context, q Query) ([]Document, error) {
	start := time.Now()
	docs, err := l.next.List(ctx, q)
	l.record(ctx, "list", "", start, err)

	if err == nil && l.log.Enabled(ctx, telemetry.LevelDebug) {
		l.log.Debug(ctx, "database list returned", telemetry.Fields{
			"count":  len(docs),
			"limit":  q.Limit,
			"offset": q.Offset,
		})
	}
	return docs, err
}
