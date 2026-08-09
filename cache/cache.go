package cache

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when an entry does not exist — including when it has
// expired, which for a cache is the same observable outcome. Providers wrap it
// with the key for context, so callers test with errors.Is rather than comparing
// formatted error strings.
var ErrNotFound = errors.New("cache: not found")

// ErrUnsupported is returned by a strategy method the backend cannot honor:
// enumeration on Memcache, a secondary index on a store with no sets.
//
// It is an error rather than a missing method because the point of the contract
// is that a call written against one backend compiles against another. The
// program still builds; it learns at the call site, from a message naming the
// backend, that this one thing is not available here.
var ErrUnsupported = errors.New("cache: unsupported by this backend")

// Config is a cache's settings, separate from the client's. The zero value is
// usable.
type Config struct {
	// Prefix namespaces every key, so several independent caches can share one
	// database, or share it with a store or a stream.
	Prefix string

	// DefaultTTL is the lease applied when an operation names none. Zero means
	// entries do not expire on their own — reasonable for [Document], rarely
	// what you want for [Volatile].
	DefaultTTL time.Duration

	// DefaultStale is the [Stale] window applied when an operation names none.
	// Zero keeps read-through blocking on expiry, which is the safe default:
	// serving stale data is a policy, not an optimization to switch on quietly.
	DefaultStale time.Duration

	// Concurrency caps how many driver calls one operation may have in flight
	// when it fans out over many keys. Zero takes a sensible default.
	//
	// It exists because the alternative to sequential round trips is not
	// unlimited ones: a List over ten thousand ids that opened ten thousand
	// concurrent calls would take the connection pool down with it.
	Concurrency int
}

// Provider is a cache backend bound to a client you own.
//
// It deliberately has no cache methods. Until a database is chosen there is
// nothing to read or write, and a provider that also answered Get would need a
// default database nobody chose — the source of a whole class of "wrote to the
// wrong one" bugs.
//
// There is no Close either: the provider did not open the client and will not
// close it. Only a [DB] can own something, and only when it had to derive it.
type Provider interface {
	// SetDatabase selects a database and returns the strategies over it. It
	// reaches the server before returning, so a bad address surfaces here rather
	// than at the first Get.
	SetDatabase(ctx context.Context, index int) (*DB, error)

	// Backend names the implementation — "redis", "memcache" — for messages.
	Backend() string
}

// DB is one database, exposing a strategy per field.
//
// The fields are contracts, so the existing Chain middleware and For typed view
// wrap any of them unchanged, and a program that switches backends keeps its
// call sites. Every field is non-nil on every backend; where one is not
// supported its methods report [ErrUnsupported].
type DB struct {
	// Document stores whole encoded values and can enumerate them.
	Document Document

	// Volatile stores values it will only hand back by key, with no index to
	// maintain and nothing to sweep.
	Volatile Volatile

	// Indexed is Document plus lookups by a field other than the id.
	Indexed Indexed

	// Aside returns a read-through view over a loader.
	//
	// It takes the loader rather than being a plain field because the loader
	// belongs to the caller: one database commonly has several, one per kind of
	// thing being cached. Written as a func field so db.Aside(load) still reads
	// as a call rather than as wiring.
	Aside func(Loader) Aside

	// Backend names the implementation behind these strategies.
	Backend string

	// Index reports which database this is.
	Index int

	// Release frees whatever selecting this database allocated, and is nil when
	// selecting it allocated nothing. Call [DB.Close] rather than this.
	Release func() error
}

// Close releases what selecting this database allocated, and nothing else.
//
// On Redis, choosing an index other than the client's means a derived client
// with a pool of its own; this closes that. The client you built is untouched —
// closing it stays your job, and doing it here would break every other database
// opened from it.
func (db *DB) Close() error {
	if db == nil || db.Release == nil {
		return nil
	}
	return db.Release()
}
