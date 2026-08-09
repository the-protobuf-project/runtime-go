package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
)

// ErrMiss is what a driver returns for a key that is not there.
//
// It is core's own sentinel rather than cache.ErrNotFound so a driver never has
// to know what the contract above it calls things — it reports what the wire
// told it, and the strategy decides whether that is a miss worth reporting or a
// step in a longer operation. Everything else a driver returns is a real failure
// and travels up untouched, so a dropped connection is never mistaken for an
// absent key.
var ErrMiss = errors.New("core: miss")

// Driver is the storage a backend must provide. Eight methods, no strategy.
//
// Every one is a single round trip against one key. Anything needing two keys or
// a decision between them is a strategy, and strategies live here rather than in
// the driver — which is the entire point of the split.
//
// # Concurrency
//
// An implementation must be safe for use by many goroutines at once, and its
// methods must not serialize against each other beyond what the connection pool
// requires. This is a contract, not a hope: core fans out over keys deliberately
// and holds no lock while it does, so a driver that guarded itself with one
// global mutex would turn every parallel read back into a queue.
//
// Both drivers here satisfy it for free, because both underlying clients are
// pools. A driver wrapping something single-threaded owes callers a pool of its
// own.
type Driver interface {
	// Name identifies the backend in error messages: "redis", "memcache".
	Name() string

	// Get returns the stored bytes, or [ErrMiss].
	Get(ctx context.Context, key string) ([]byte, error)

	// Set writes unconditionally. A zero ttl means no expiry.
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// Add writes only if the key is absent, reporting whether it wrote. This is
	// SET NX on Redis and add on Memcache, and it is what makes a cross-process
	// lock possible without a second system.
	Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error)

	// Replace writes only if the key is present, reporting whether it wrote.
	//
	// A strategy could read first and then write, but between the two the entry
	// can expire and the write would recreate what the caller believed it was
	// updating. One conditional round trip closes that window.
	Replace(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error)

	// Delete removes keys. Removing an absent key is not an error.
	Delete(ctx context.Context, keys ...string) error

	// Exists reports whether a key is live, without paying to transfer its
	// value — which matters because sweeping an index asks this per member.
	Exists(ctx context.Context, key string) (bool, error)

	// Touch extends a lease without rewriting the value.
	Touch(ctx context.Context, key string, ttl time.Duration) error
}

// unsupported reports a capability the backend does not have, naming it, so the
// message reads as a fact about the backend rather than a missing feature.
func unsupported(driver, op, why string) error {
	return fmt.Errorf("%w: %s cannot %s (%s)", cache.ErrUnsupported, driver, op, why)
}

// checkTTL rejects a write that resolved to no expiry where the cache was
// configured to insist on one.
//
// It runs before the write rather than after, and before a read-through loader
// rather than after: a call that is going to be refused should not first spend a
// round trip or run somebody's loader to find that out.
func checkTTL(require bool, o cache.Options, op, id string) error {
	if !require || o.TTL > 0 || o.Permanent {
		return nil
	}
	where := op
	if id != "" {
		where = fmt.Sprintf("%s for %q", op, id)
	}
	return fmt.Errorf(
		"%w: %s; pass cache.TTL(d), set Config.DefaultTTL, or state it deliberately with cache.NoExpiry()",
		cache.ErrNoTTL, where)
}

// notFound turns a driver [ErrMiss] into the contract's sentinel and leaves every
// other failure alone.
func notFound(id string, err error) error {
	if errors.Is(err, ErrMiss) {
		return fmt.Errorf("%w: %s", cache.ErrNotFound, id)
	}
	return err
}

// isNotFound reports whether an error is a miss in either vocabulary, for the
// several places that skip an entry which expired mid-operation.
func isNotFound(err error) bool {
	return errors.Is(err, ErrMiss) || errors.Is(err, cache.ErrNotFound)
}
