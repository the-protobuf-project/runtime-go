package store

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// Typed is a view over a [Driver] bound to one resource and one message type.
//
// It is a wrapper, not a separate client: the underlying driver is dynamic and
// shared, so one connection serves every resource in a program. Build as many
// views over it as you have resources.
//
// The point is that it removes the two arguments a caller repeats on every call
// and can get wrong — the descriptor and the type assertion on the way out:
//
//	books := For[*pb.Book](driver, res)
//	b, err := books.Get(ctx, "books/dune")   // already a *pb.Book
//
// against the dynamic form, where passing the wrong resource for the message is
// a runtime error and every read ends in an assertion the compiler cannot check.
type Typed[T proto.Message] struct {
	d   Driver
	res *Resource
}

// For returns a view of d bound to res, reading and writing T.
//
// The pairing is checked once here rather than on every call: res.New must
// produce a T, so a descriptor and a message type that do not belong together
// fail at wiring time instead of on the first Get in a request handler.
func For[T proto.Message](d Driver, res *Resource) (Typed[T], error) {
	if d == nil {
		return Typed[T]{}, fmt.Errorf("store: For needs a driver")
	}
	if res == nil {
		return Typed[T]{}, fmt.Errorf("store: For needs a resource")
	}
	if res.New == nil {
		return Typed[T]{}, fmt.Errorf("store: resource %q has no New; it cannot allocate a message", res.Name)
	}
	if _, ok := res.New().(T); !ok {
		var zero T
		return Typed[T]{}, fmt.Errorf(
			"store: resource %q allocates %T, not %T — the descriptor and the type do not match",
			res.Name, res.New(), zero)
	}
	return Typed[T]{d: d, res: res}, nil
}

// MustFor is [For] for a pairing settled at build time, panicking rather than
// returning an error.
//
// It is for a package-level variable wired from generated descriptors, where a
// mismatch is a build mistake and there is no caller to hand an error to. Use
// [For] anywhere the resource comes from configuration.
func MustFor[T proto.Message](d Driver, res *Resource) Typed[T] {
	t, err := For[T](d, res)
	if err != nil {
		panic(err)
	}
	return t
}

// Unwrap returns the underlying driver and descriptor, for the operations the
// view does not cover.
func (t Typed[T]) Unwrap() (Driver, *Resource) { return t.d, t.res }

// Create stores msg as a new record.
func (t Typed[T]) Create(ctx context.Context, msg T) (WriteResult, error) {
	return t.d.Create(ctx, t.res, msg)
}

// Get returns the record under key, already of type T.
func (t Typed[T]) Get(ctx context.Context, key string) (T, error) {
	var zero T
	msg, err := t.d.Get(ctx, t.res, key)
	if err != nil {
		return zero, err
	}
	out, ok := msg.(T)
	if !ok {
		return zero, fmt.Errorf("store: %s returned %T, not %T", t.res.Name, msg, zero)
	}
	return out, nil
}

// Update overwrites the record identified by msg's primary key.
func (t Typed[T]) Update(ctx context.Context, msg T) (WriteResult, error) {
	return t.d.Update(ctx, t.res, msg)
}

// Delete removes the record under key.
func (t Typed[T]) Delete(ctx context.Context, key string) error {
	return t.d.Delete(ctx, t.res, key)
}

// Exists reports whether a record with the given key is there.
func (t Typed[T]) Exists(ctx context.Context, key string) (bool, error) {
	return t.d.Exists(ctx, t.res, key)
}

// Count returns how many records match opts.Filter.
func (t Typed[T]) Count(ctx context.Context, opts ListOptions) (int64, error) {
	return t.d.Count(ctx, t.res, opts)
}

// List returns a page of records as T, with the continuation token and total.
//
// The slice is typed, so the assertion every caller would otherwise write per
// item happens once, here, and a backend returning the wrong message type is
// reported as that rather than as a panic in the caller's loop.
func (t Typed[T]) List(ctx context.Context, opts ListOptions) ([]T, string, int64, error) {
	res, err := t.d.List(ctx, t.res, opts)
	if err != nil {
		return nil, "", 0, err
	}
	out := make([]T, 0, len(res.Items))
	for i, msg := range res.Items {
		item, ok := msg.(T)
		if !ok {
			var zero T
			return nil, "", 0, fmt.Errorf(
				"store: %s returned %T at index %d, not %T", t.res.Name, msg, i, zero)
		}
		out = append(out, item)
	}
	return out, res.NextPageToken, res.Total, nil
}

// All pages through every record matching opts and returns them.
//
// It exists because the paging loop is the same four lines every time and one of
// them is easy to get wrong — a driver that returns a full page with no next
// token ends an incorrect loop early, and one that repeats a token spins
// forever. This stops on an empty token and refuses to follow a token it has
// already followed.
//
// It is still a loop over pages: a resource with a million records costs a
// million records of memory. Use [Typed.List] when that matters.
func (t Typed[T]) All(ctx context.Context, opts ListOptions) ([]T, error) {
	var (
		out  []T
		seen = map[string]bool{}
	)
	for {
		page, next, _, err := t.List(ctx, opts)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if next == "" {
			return out, nil
		}
		if seen[next] {
			return nil, fmt.Errorf("store: %s repeated page token %q; refusing to loop", t.res.Name, next)
		}
		seen[next] = true
		opts.PageToken = next
	}
}
