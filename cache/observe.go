package cache

import (
	"context"
	"errors"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// WithObservability returns next with every operation traced, measured and
// logged through signals, and next unchanged when signals has none.
//
// Each call is a [telemetry.Operation] of component "cache", backend "cache":
// a client span named cache.<operation> (create, get, update, delete, keys,
// list, ttl, and by_index, ids_by_index, delete_by_index on an [Indexed]
// cache), and a share of runtime.operation.count and
// runtime.operation.duration, labeled component, backend, operation and
// outcome. A miss is the not_found outcome, which does not fail the span: an
// absent key is a normal cache answer. Keys and values are never recorded.
// It is runtime-rs's cache::with_observability, operation for operation.
//
// An [Indexed] cache stays Indexed. Wrapper order decides what is measured: put
// this outside [WithRetry] to time a logical operation, inside to time each
// attempt.
func WithObservability(next Document, signals telemetry.Signals) Document {
	if !signals.Enabled() {
		return next
	}
	o := &observed{obs: signals.Observer("cache")}
	doc := &observedDocument{o: o, next: next}
	if ix, ok := next.(Indexed); ok {
		return &observedIndexed{observedDocument: doc, ix: ix}
	}
	return doc
}

// WithObservabilityMiddleware is [WithObservability] as a [Middleware].
func WithObservabilityMiddleware(signals telemetry.Signals) Middleware {
	return func(c Document) Document { return WithObservability(c, signals) }
}

// WithVolatileObservability is [WithObservability] for a [Volatile] cache:
// set, get, delete, touch, ttl and scan.
func WithVolatileObservability(next Volatile, signals telemetry.Signals) Volatile {
	if !signals.Enabled() {
		return next
	}
	return &observedVolatile{o: &observed{obs: signals.Observer("cache")}, next: next}
}

// WithAsideObservability is [WithObservability] for an [Aside] cache:
// get_or_load, refresh and invalidate. Instrument the Document underneath as
// well to see whether a get_or_load was answered from the cache or loaded.
func WithAsideObservability(next Aside, signals telemetry.Signals) Aside {
	if !signals.Enabled() {
		return next
	}
	return &observedAside{o: &observed{obs: signals.Observer("cache")}, next: next}
}

// observed starts the operations of one cache.
type observed struct {
	obs *telemetry.Observer
}

// run reports fn as one operation.
func (o *observed) run(ctx context.Context, operation string, written int, fn func(ctx context.Context) (read int, err error)) error {
	ctx, op := o.obs.Start(ctx, "cache", operation, telemetry.SpanKindClient)
	read, err := fn(ctx)
	if err == nil {
		op.ReadRecords(read)
		op.WrittenRecords(written)
	}
	outcome := classify(err)
	if outcome.IsError() {
		op.Span().SetError(err)
	}
	op.Finish(outcome)
	return err
}

// classify is the outcome of a cache error.
func classify(err error) telemetry.Outcome {
	switch {
	case err == nil:
		return telemetry.OutcomeOK
	case errors.Is(err, ErrNotFound):
		return telemetry.OutcomeNotFound
	case errors.Is(err, ErrUnsupported):
		return telemetry.OutcomeUnsupported
	case errors.Is(err, ErrNoTTL):
		return telemetry.OutcomeInvalid
	case errors.Is(err, ErrOverloaded):
		return telemetry.OutcomeClosed
	default:
		return telemetry.OutcomeOf(err)
	}
}

type observedDocument struct {
	o    *observed
	next Document
}

func (d *observedDocument) Create(ctx context.Context, value any, opts ...Option) (id string, err error) {
	err = d.o.run(ctx, "create", 1, func(ctx context.Context) (int, error) {
		id, err = d.next.Create(ctx, value, opts...)
		return 0, err
	})
	return id, err
}

func (d *observedDocument) Get(ctx context.Context, id string, dest any) error {
	return d.o.run(ctx, "get", 0, func(ctx context.Context) (int, error) {
		return 1, d.next.Get(ctx, id, dest)
	})
}

func (d *observedDocument) Update(ctx context.Context, id string, value any, opts ...Option) error {
	return d.o.run(ctx, "update", 1, func(ctx context.Context) (int, error) {
		return 0, d.next.Update(ctx, id, value, opts...)
	})
}

func (d *observedDocument) Delete(ctx context.Context, id string) error {
	return d.o.run(ctx, "delete", 0, func(ctx context.Context) (int, error) {
		return 0, d.next.Delete(ctx, id)
	})
}

func (d *observedDocument) Keys(ctx context.Context) (keys []string, err error) {
	err = d.o.run(ctx, "keys", 0, func(ctx context.Context) (int, error) {
		keys, err = d.next.Keys(ctx)
		return 0, err
	})
	return keys, err
}

func (d *observedDocument) List(ctx context.Context, dest any) error {
	return d.o.run(ctx, "list", 0, func(ctx context.Context) (int, error) {
		return 0, d.next.List(ctx, dest)
	})
}

func (d *observedDocument) TTL(ctx context.Context, id string) (ttl time.Duration, err error) {
	err = d.o.run(ctx, "ttl", 0, func(ctx context.Context) (int, error) {
		ttl, err = d.next.TTL(ctx, id)
		return 0, err
	})
	return ttl, err
}

type observedIndexed struct {
	*observedDocument
	ix Indexed
}

func (d *observedIndexed) ByIndex(ctx context.Context, field, value string, dest any) error {
	return d.o.run(ctx, "by_index", 0, func(ctx context.Context) (int, error) {
		return 0, d.ix.ByIndex(ctx, field, value, dest)
	})
}

func (d *observedIndexed) IDsByIndex(ctx context.Context, field, value string) (ids []string, err error) {
	err = d.o.run(ctx, "ids_by_index", 0, func(ctx context.Context) (int, error) {
		ids, err = d.ix.IDsByIndex(ctx, field, value)
		return len(ids), err
	})
	return ids, err
}

func (d *observedIndexed) DeleteByIndex(ctx context.Context, field, value string) (n int, err error) {
	err = d.o.run(ctx, "delete_by_index", 0, func(ctx context.Context) (int, error) {
		n, err = d.ix.DeleteByIndex(ctx, field, value)
		return 0, err
	})
	return n, err
}

type observedVolatile struct {
	o    *observed
	next Volatile
}

func (v *observedVolatile) Set(ctx context.Context, key string, value any, opts ...Option) error {
	return v.o.run(ctx, "set", 1, func(ctx context.Context) (int, error) {
		return 0, v.next.Set(ctx, key, value, opts...)
	})
}

func (v *observedVolatile) Get(ctx context.Context, key string, dest any) error {
	return v.o.run(ctx, "get", 0, func(ctx context.Context) (int, error) {
		return 1, v.next.Get(ctx, key, dest)
	})
}

func (v *observedVolatile) Delete(ctx context.Context, key string) error {
	return v.o.run(ctx, "delete", 0, func(ctx context.Context) (int, error) {
		return 0, v.next.Delete(ctx, key)
	})
}

func (v *observedVolatile) Touch(ctx context.Context, key string, ttl time.Duration) error {
	return v.o.run(ctx, "touch", 0, func(ctx context.Context) (int, error) {
		return 0, v.next.Touch(ctx, key, ttl)
	})
}

func (v *observedVolatile) TTL(ctx context.Context, key string) (ttl time.Duration, err error) {
	err = v.o.run(ctx, "ttl", 0, func(ctx context.Context) (int, error) {
		ttl, err = v.next.TTL(ctx, key)
		return 0, err
	})
	return ttl, err
}

func (v *observedVolatile) Scan(ctx context.Context, pattern string) (keys []string, err error) {
	err = v.o.run(ctx, "scan", 0, func(ctx context.Context) (int, error) {
		keys, err = v.next.Scan(ctx, pattern)
		return len(keys), err
	})
	return keys, err
}

type observedAside struct {
	o    *observed
	next Aside
}

func (a *observedAside) GetOrLoad(ctx context.Context, id string, dest any, opts ...Option) error {
	return a.o.run(ctx, "get_or_load", 0, func(ctx context.Context) (int, error) {
		return 1, a.next.GetOrLoad(ctx, id, dest, opts...)
	})
}

func (a *observedAside) Refresh(ctx context.Context, id string, opts ...Option) error {
	return a.o.run(ctx, "refresh", 1, func(ctx context.Context) (int, error) {
		return 0, a.next.Refresh(ctx, id, opts...)
	})
}

func (a *observedAside) Invalidate(ctx context.Context, id string) error {
	return a.o.run(ctx, "invalidate", 0, func(ctx context.Context) (int, error) {
		return 0, a.next.Invalidate(ctx, id)
	})
}

var (
	_ Document = (*observedDocument)(nil)
	_ Indexed  = (*observedIndexed)(nil)
	_ Volatile = (*observedVolatile)(nil)
	_ Aside    = (*observedAside)(nil)
)
