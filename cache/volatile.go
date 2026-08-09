package cache

import (
	"context"
	"time"
)

// Volatile is TTL-first storage with no index.
//
// The lease is the point: a session, a rate-limit window, a memoized response.
// Because nothing tracks what is stored, a write is one round trip, a read
// sweeps nothing, and entries expiring costs exactly nothing. What you give up
// is enumeration — [Volatile.Scan] is best-effort and some backends refuse it.
//
// Keys here are yours, not generated. A caller reaching for Volatile already
// knows the key it wants: the session id, the user id, the request hash.
//
// It is also the strategy that scales furthest. With no index there is no key
// every write has to touch, so entries spread across a cluster by their own
// names and adding shards adds throughput — which is not true of [Document] or
// [Indexed].
type Volatile interface {
	// Set writes value under key with a lease.
	Set(ctx context.Context, key string, value any, opts ...Option) error

	// Get decodes the entry into dest. It returns an error wrapping
	// [ErrNotFound] when the key is absent or its lease has run out.
	Get(ctx context.Context, key string, dest any) error

	// Delete removes an entry; removing an absent one is not an error.
	Delete(ctx context.Context, key string) error

	// Touch extends an entry's lease without rewriting its value, which is what
	// a sliding session window needs and what re-Setting the value to get it
	// would make needlessly expensive.
	Touch(ctx context.Context, key string, ttl time.Duration) error

	// TTL reports how much longer an entry will live, or [ErrUnsupported] where
	// the protocol never reports one.
	TTL(ctx context.Context, key string) (time.Duration, error)

	// Scan returns the keys matching a glob pattern.
	//
	// Best-effort and never transactional: keys may expire mid-walk and the
	// result is a snapshot of nothing in particular. It exists for operators
	// poking at a cache, not for application logic. Backends that cannot walk
	// their keyspace report [ErrUnsupported].
	Scan(ctx context.Context, pattern string) ([]string, error)
}
