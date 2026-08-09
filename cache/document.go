package cache

import (
	"context"
	"time"
)

// Document is ephemeral storage for whole values, enumerable.
//
// A value goes in as it is and comes back decoded into a destination you own, so
// adding a field to your model is not a change to this package.
//
// Enumeration is not free. A backend that cannot list a logical group without
// walking its whole keyspace has to maintain an index, which makes Create two
// writes and makes reads sweep the members whose entries have since expired.
// Reach for [Volatile] when you will never enumerate.
//
// # It does not shard
//
// The index is one key. On a Redis cluster that key lives on one node, so every
// Create and Delete here touches that node no matter how many are in the ring —
// adding shards buys this strategy nothing, and the write rate one node sustains
// is the write rate of the whole cache.
//
// [Document.Keys] and [Document.List] are O(entries) besides: a cursor keeps the
// server from stalling on one enormous reply, and batching keeps the round trips
// proportional to entries over a few hundred, but nothing makes them cheap at a
// million.
//
// So this is the strategy for thousands to low millions of entries at a modest
// write rate. Past that the shape to reach for is [Volatile] or [Aside], which
// have no index, no hot key, and nothing that has to be swept.
type Document interface {
	// Create stores value under a generated id and returns it. Pass [ID] to
	// choose the id yourself.
	Create(ctx context.Context, value any, opts ...Option) (string, error)

	// Get decodes the entry into dest, which must be a non-nil pointer. It
	// returns an error wrapping [ErrNotFound] when no such entry exists or it
	// has expired.
	Get(ctx context.Context, id string, dest any) error

	// Update replaces the value stored under id. It returns an error wrapping
	// [ErrNotFound] when the entry does not exist, rather than creating it.
	Update(ctx context.Context, id string, value any, opts ...Option) error

	// Delete removes an entry. Deleting one that is not there is not an error —
	// the caller's intent is already satisfied, and a cache entry may
	// legitimately have expired a moment earlier.
	Delete(ctx context.Context, id string) error

	// Keys returns the ids of every live entry, sweeping ones that have expired.
	// It reports [ErrUnsupported] on a backend with no sets to index with.
	Keys(ctx context.Context) ([]string, error)

	// List decodes every live entry into dest, which must be a non-nil pointer
	// to a slice.
	List(ctx context.Context, dest any) error

	// TTL reports how much longer an entry will live. It returns zero for an
	// entry with no expiry, an error wrapping [ErrNotFound] when the entry does
	// not exist, and [ErrUnsupported] on a backend whose protocol never reports
	// a remaining lease.
	TTL(ctx context.Context, id string) (time.Duration, error)
}
