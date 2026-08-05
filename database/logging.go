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

func (l *loggingStore) Create(ctx context.Context, id string, value any, opts ...Option) (string, error) {
	start := time.Now()
	out, err := l.next.Create(ctx, id, value, opts...)

	// Create mints an id when the caller supplies none; log the one that was
	// actually used.
	logged := id
	if out != "" {
		logged = out
	}
	l.record(ctx, "create", logged, start, err)

	// A returned id that differs from the requested one means the content was
	// already stored — worth surfacing, since the caller's write did not
	// produce a new record.
	if err == nil && id != "" && out != id {
		l.log.Info(ctx, "database create deduplicated", telemetry.Fields{
			"requested": id,
			"existing":  out,
		})
	}
	return out, err
}

func (l *loggingStore) Get(ctx context.Context, id string, dest any) error {
	start := time.Now()
	err := l.next.Get(ctx, id, dest)
	l.record(ctx, "get", id, start, err)
	return err
}

func (l *loggingStore) Update(ctx context.Context, id string, value any, opts ...Option) error {
	start := time.Now()
	err := l.next.Update(ctx, id, value, opts...)
	l.record(ctx, "update", id, start, err)
	return err
}

func (l *loggingStore) Delete(ctx context.Context, id string) error {
	start := time.Now()
	err := l.next.Delete(ctx, id)
	l.record(ctx, "delete", id, start, err)
	return err
}

func (l *loggingStore) Keys(ctx context.Context, opts ...Option) ([]string, error) {
	start := time.Now()
	keys, err := l.next.Keys(ctx, opts...)
	l.record(ctx, "keys", "", start, err)

	if err == nil && l.log.Enabled(ctx, telemetry.LevelDebug) {
		o := NewOptions(opts...)
		l.log.Debug(ctx, "database keys returned", telemetry.Fields{
			"count":  len(keys),
			"limit":  o.Limit,
			"offset": o.Offset,
		})
	}
	return keys, err
}

func (l *loggingStore) List(ctx context.Context, dest any, opts ...Option) error {
	start := time.Now()
	err := l.next.List(ctx, dest, opts...)
	l.record(ctx, "list", "", start, err)
	return err
}
