package core

import (
	"context"
	"time"
)

// Backends are not equally capable, and pretending otherwise is how a contract
// starts lying. Each interface below is something a driver implements only if it
// can; core asserts for them once at construction and adapts.
//
// Two kinds live here. [Sets], [Leases] and [Scanner] gate behavior — without
// them a strategy refuses. [Bulk] and [Fenced] gate quality — without them core
// still works, by a slower or weaker route it documents.

// Sets is implemented by a driver with server-side sets.
//
// It is the capability that makes a group of ids addressable, so a backend
// without it cannot enumerate and cannot index. Faking it — one key holding a
// serialized list, rewritten under a compare-and-swap loop — puts every write in
// the cache in contention on one key and silently drops ids when a race is lost,
// which is worse than saying no.
type Sets interface {
	SetAdd(ctx context.Context, key string, members ...string) error
	SetRemove(ctx context.Context, key string, members ...string) error
	SetMembers(ctx context.Context, key string) ([]string, error)
}

// Leases is implemented by a driver whose protocol reports a remaining TTL.
//
// Memcache stores an expiry and honors it but will never say what is left of it,
// so code that renews a lease as it runs low cannot be written against that
// backend at all. A real behavioral gap, not an inconvenience, and the reason
// this is a capability rather than a required method returning zero.
type Leases interface {
	TTL(ctx context.Context, key string) (time.Duration, error)
}

// Scanner is implemented by a driver that can walk its keyspace with a cursor.
type Scanner interface {
	Scan(ctx context.Context, pattern string) ([]string, error)
}

// Bulk is implemented by a driver that can answer about many keys in one round
// trip — a pipeline on Redis, a multi-get on Memcache.
//
// This is the difference between a listing of ten thousand entries costing ten
// thousand network hops and costing forty. Without it core still fans out, with
// bounded concurrency, which recovers most of the latency but none of the
// syscalls; with it, both.
//
// Implementations return one result per key, in the order asked. A nil entry in
// GetMany is a miss — not an error, because a listing racing an expiry is
// ordinary and the caller decides what to do about it.
type Bulk interface {
	GetMany(ctx context.Context, keys []string) ([][]byte, error)
	ExistsMany(ctx context.Context, keys []string) ([]bool, error)
}

// Fenced is implemented by a driver that can delete a key only when it still
// holds an expected value.
//
// It exists for exactly one thing: releasing a lock. An unconditional delete
// releases whatever lock is there, which after a lease expires is somebody
// else's — so a slow loader finishing late hands the key to a third goroutine
// while a second one is still working under a lock it believes it holds. That
// bug is rare, unreproducible, and produces duplicate loads at the worst moment.
//
// A driver without this does not get a cross-process lock at all; core falls
// back to per-process collapsing, which is correct, needs no lock, and bounds
// loads by the number of processes rather than the number of requests.
type Fenced interface {
	DeleteIf(ctx context.Context, key string, value []byte) (bool, error)
}
