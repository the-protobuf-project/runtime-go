package cache

import (
	"context"
	"time"
)

// Typed is a view over a [Document] bound to one model.
//
// It is a wrapper, not a separate client: the underlying Document is untyped and
// shared, so one provider — one connection, one configuration — serves every
// model in a program. Build as many views over it as you have types.
type Typed[T any] struct {
	c Document
}

// For returns a view of c that reads and writes T.
//
//	users := cache.For[User](db.Document)
//	id, _ := users.Create(ctx, u, cache.TTL(time.Minute))
//	u2, _ := users.Get(ctx, id)
//
// Create names the entry itself; [Typed.Put] takes an id you chose.
//
// Nothing about the underlying Document changes, so views of different types over
// the same Document see the same entries. Give each model its own provider —
// separate prefixes or databases — when they should not.
func For[T any](c Document) Typed[T] {
	return Typed[T]{c: c}
}

// Unwrap returns the underlying Document, for the operations the view does not
// cover or to hand to something expecting the untyped contract.
func (t Typed[T]) Unwrap() Document { return t.c }

// Create stores value under a generated id and returns it. Use [Typed.Put] to
// choose the id yourself.
func (t Typed[T]) Create(ctx context.Context, value T, opts ...Option) (string, error) {
	return t.c.Create(ctx, value, opts...)
}

// Put stores value under an id you choose, returning the id used. It is
// [Typed.Create] with [ID] already applied, for when naming the entry is the
// point of the call rather than an adjustment to it.
//
// The id argument wins over any [ID] option in opts: it is the more specific
// statement, and a call that names an id twice should not depend on which one
// the reader notices first.
func (t Typed[T]) Put(ctx context.Context, id string, value T, opts ...Option) (string, error) {
	settled := make([]Option, 0, len(opts)+1)
	settled = append(settled, opts...)
	settled = append(settled, ID(id))
	return t.c.Create(ctx, value, settled...)
}

// Get returns the entry stored under id, decoded as T. It returns an error
// wrapping [ErrNotFound] when no such entry exists or it has expired.
func (t Typed[T]) Get(ctx context.Context, id string) (T, error) {
	var out T
	if err := t.c.Get(ctx, id, &out); err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}

// Update replaces the value stored under id.
func (t Typed[T]) Update(ctx context.Context, id string, value T, opts ...Option) error {
	return t.c.Update(ctx, id, value, opts...)
}

// Delete removes an entry.
func (t Typed[T]) Delete(ctx context.Context, id string) error {
	return t.c.Delete(ctx, id)
}

// Keys returns the ids of every live entry.
func (t Typed[T]) Keys(ctx context.Context) ([]string, error) {
	return t.c.Keys(ctx)
}

// List returns every live entry, decoded as T.
func (t Typed[T]) List(ctx context.Context) ([]T, error) {
	var out []T
	if err := t.c.List(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// TTL reports how much longer an entry will live; zero means it does not
// expire.
func (t Typed[T]) TTL(ctx context.Context, id string) (time.Duration, error) {
	return t.c.TTL(ctx, id)
}
