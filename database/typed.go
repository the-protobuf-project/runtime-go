package database

import "context"

// Typed is a view over a [Store] bound to one model.
//
// It is a wrapper, not a separate client: the underlying Store is untyped and
// shared, so one provider — one connection, one configuration — serves every
// model in a program. Build as many views over it as you have types.
type Typed[T any] struct {
	s Store
}

// For returns a view of s that reads and writes T.
//
//	books := database.For[Book](mgr.Document.KV)
//	id, _ := books.Create(ctx, b)
//	b2, _ := books.Get(ctx, id)
//
// Nothing about the underlying Store changes, so views of different types over
// the same Store see the same records. Give each model its own provider —
// separate prefixes or databases — when they should not.
func For[T any](s Store) Typed[T] {
	return Typed[T]{s: s}
}

// Unwrap returns the underlying Store, for the operations the view does not
// cover or to hand to something expecting the untyped contract.
func (t Typed[T]) Unwrap() Store { return t.s }

// Create stores value under a generated id and returns it. Use [Typed.Put] to
// choose the id yourself.
//
// A provider that deduplicates by content returns the id of an existing record
// rather than storing a copy.
func (t Typed[T]) Create(ctx context.Context, value T, opts ...Option) (string, error) {
	return t.s.Create(ctx, "", value, opts...)
}

// Put stores value under id, returning the id used.
func (t Typed[T]) Put(ctx context.Context, id string, value T, opts ...Option) (string, error) {
	return t.s.Create(ctx, id, value, opts...)
}

// Get returns the record stored under id, decoded as T. It returns an error
// wrapping [ErrNotFound] when no such record exists.
func (t Typed[T]) Get(ctx context.Context, id string) (T, error) {
	var out T
	if err := t.s.Get(ctx, id, &out); err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}

// Update replaces the value stored under id.
func (t Typed[T]) Update(ctx context.Context, id string, value T, opts ...Option) error {
	return t.s.Update(ctx, id, value, opts...)
}

// Delete removes a record.
func (t Typed[T]) Delete(ctx context.Context, id string) error {
	return t.s.Delete(ctx, id)
}

// Keys returns the ids of every stored record, in a stable order.
func (t Typed[T]) Keys(ctx context.Context, opts ...Option) ([]string, error) {
	return t.s.Keys(ctx, opts...)
}

// List returns stored records decoded as T, in a stable order.
func (t Typed[T]) List(ctx context.Context, opts ...Option) ([]T, error) {
	var out []T
	if err := t.s.List(ctx, &out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}
