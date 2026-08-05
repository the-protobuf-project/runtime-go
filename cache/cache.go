package cache

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when an entry does not exist — including when it has
// expired, which for a cache is the same observable outcome. Providers wrap it
// with the key for context, so callers test with errors.Is rather than
// comparing formatted error strings.
var ErrNotFound = errors.New("cache: not found")

// Options are the per-operation settings a provider understands. A provider
// ignores what it cannot honor, so a call written against one backend still
// compiles and runs against another.
type Options struct {
	// TTL is how long the entry lives. Zero means it does not expire.
	TTL time.Duration
}

// Option configures one operation.
//
// Settings are options rather than parameters so a provider can gain or lose a
// capability without changing any signature — which is the point when several
// caches implement this contract.
type Option func(*Options)

// TTL sets how long an entry lives. Zero means it does not expire.
func TTL(d time.Duration) Option {
	return func(o *Options) { o.TTL = d }
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

// Cache is the contract every provider satisfies.
//
// Every method takes a context: these are network calls, and the caller decides
// how long to wait for them and when to give up.
type Cache interface {
	// Create stores value under id, or under a generated id when id is empty,
	// and returns the id it used.
	Create(ctx context.Context, id string, value any, opts ...Option) (string, error)

	// Get decodes the entry into dest, which must be a non-nil pointer. It
	// returns an error wrapping [ErrNotFound] when no such entry exists or it
	// has expired.
	Get(ctx context.Context, id string, dest any) error

	// Update replaces the value stored under id. It returns an error wrapping
	// [ErrNotFound] when the entry does not exist.
	Update(ctx context.Context, id string, value any, opts ...Option) error

	// Delete removes an entry. Deleting one that is not there is not an error —
	// the caller's intent is already satisfied, and a cache entry may
	// legitimately have expired a moment earlier.
	Delete(ctx context.Context, id string) error

	// Keys returns the ids of every live entry, sweeping ones that have expired.
	Keys(ctx context.Context) ([]string, error)

	// List decodes every live entry into dest, which must be a non-nil pointer
	// to a slice.
	List(ctx context.Context, dest any) error

	// TTL reports how much longer an entry will live. It returns zero for an
	// entry with no expiry, and an error wrapping [ErrNotFound] when the entry
	// does not exist.
	TTL(ctx context.Context, id string) (time.Duration, error)
}

// Closer is implemented by providers holding a resource of their own that must
// be released. Providers do not own the client they were handed, so most have
// nothing to close — closing that client stays the caller's job.
type Closer interface {
	Close() error
}
