package database

import (
	"context"
	"errors"
)

var (
	// ErrNotFound is returned when a record does not exist. Providers wrap it
	// with the id for context, so callers test with errors.Is rather than
	// comparing formatted error strings.
	ErrNotFound = errors.New("database: not found")

	// ErrDuplicate is returned when a write would store content that already
	// exists under a different id.
	//
	// Providers that deduplicate by content report this instead of silently
	// creating a second copy — see [Store.Create].
	ErrDuplicate = errors.New("database: duplicate content")
)

// Options are the per-operation settings a provider understands. A provider
// ignores what it cannot honor, so a call written against one backend still
// compiles and runs against another.
type Options struct {
	// Limit caps how many records a read returns. Zero means no limit.
	Limit int

	// Offset skips this many records before collecting results. It is only
	// meaningful alongside a stable order, which providers guarantee by
	// sorting ids.
	Offset int
}

// Option configures one operation.
//
// Settings are options rather than parameters so a provider can gain or lose a
// capability without changing any signature — which is the point when several
// stores implement this contract.
type Option func(*Options)

// Limit caps how many records a read returns.
func Limit(n int) Option {
	return func(o *Options) { o.Limit = n }
}

// Offset skips this many records before collecting results.
func Offset(n int) Option {
	return func(o *Options) { o.Offset = n }
}

// NewOptions folds opts into a single Options. Providers call this rather than
// re-implementing the same loop.
func NewOptions(opts ...Option) Options {
	var o Options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Store is the contract every provider satisfies.
//
// Every method takes a context: these are network calls, and the caller decides
// how long to wait and when to give up.
type Store interface {
	// Create stores value under id, or under a generated id when id is empty,
	// and returns the id it used.
	//
	// Providers that deduplicate by content return the id of the existing
	// record rather than storing a second copy — compare the returned id
	// against the one you supplied to tell the two apart.
	Create(ctx context.Context, id string, value any, opts ...Option) (string, error)

	// Get decodes the record into dest, which must be a non-nil pointer. It
	// returns an error wrapping [ErrNotFound] when no such record exists.
	Get(ctx context.Context, id string, dest any) error

	// Update replaces the value stored under id. It returns an error wrapping
	// [ErrNotFound] when the record does not exist, and one wrapping
	// [ErrDuplicate] when the new value already belongs to another record.
	Update(ctx context.Context, id string, value any, opts ...Option) error

	// Delete removes a record. It returns an error wrapping [ErrNotFound] when
	// the record does not exist — unlike a cache, a missing record here is a
	// genuine surprise worth reporting.
	Delete(ctx context.Context, id string) error

	// Keys returns the ids of every stored record, in a stable order.
	Keys(ctx context.Context, opts ...Option) ([]string, error)

	// List decodes stored records into dest, which must be a non-nil pointer to
	// a slice, in the same stable order as [Store.Keys].
	List(ctx context.Context, dest any, opts ...Option) error
}

// Closer is implemented by providers holding a resource of their own that must
// be released. Providers do not own the client they were handed, so most have
// nothing to close — closing that client stays the caller's job.
type Closer interface {
	Close() error
}
