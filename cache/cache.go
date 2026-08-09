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

// ErrNoTTL is returned by a write that resolved to no expiry in a cache
// configured with [Config.RequireTTL].
//
// It exists because the unsafe case is silence, not intent. An entry meant to
// outlive every lease is fine — say so with [NoExpiry] and this never fires.
// What it catches is the write that meant to have a lease and forgot, which
// otherwise succeeds, looks correct, and turns a cache into a store that only
// grows.
var ErrNoTTL = errors.New("cache: no expiry, and this cache requires one")

// ErrOverloaded is returned by [Aside] when too many distinct loads are already
// running and starting another would not be bounded by anything.
//
// It is a refusal rather than a wait because the alternative is worse. Loads
// grow with distinct ids, not with callers — a cold start, or a client walking
// ids nobody has asked for — and one goroutine per id with no ceiling is not a
// slow cache but a dead process. Joining a load already running is never
// refused, so a hot key is unaffected however many callers arrive.
//
// A caller seeing this can fall back to its own loader, shed the request, or
// retry: the cache is saying it will not queue work it cannot bound, not that
// the data is gone.
var ErrOverloaded = errors.New("cache: too many concurrent loads")

// Config is a cache's settings, separate from the client's. The zero value is
// usable.
type Config struct {
	// Prefix namespaces every key, so several independent caches can share one
	// database, or share it with a store or a stream.
	Prefix string

	// DefaultTTL is the lease applied when an operation names none. Zero means
	// entries do not expire on their own — reasonable for [Document], rarely
	// what you want for [Volatile], and close to never what you want for
	// [Aside], which would then keep every id it ever loaded.
	DefaultTTL time.Duration

	// RequireTTL makes a write that resolved to no expiry an error rather than a
	// permanent entry. Off by default.
	//
	// Turn it on and forgetting a lease fails at the first write, naming the
	// operation, instead of surviving review and surfacing weeks later as memory
	// that only grows. An entry that genuinely should outlive every lease is
	// still allowed: state it with [NoExpiry], which this deliberately does not
	// block. The target is silence, not intent.
	RequireTTL bool

	// DefaultStale is the [Stale] window applied when an operation names none.
	// Zero keeps read-through blocking on expiry, which is the safe default:
	// serving stale data is a policy, not an optimization to switch on quietly.
	DefaultStale time.Duration

	// Databases is the set of names [Provider.SetDatabase] will accept. Empty
	// means any name is allowed.
	//
	// It exists because a namespace has no existence to check: SetDatabase with a
	// typo succeeds and hands back a working, empty cache, and the mistake shows
	// up as a cache that never hits rather than as an error. Listing the names a
	// program uses turns that into a refusal at startup.
	//
	// Deliberately a slice here and not a registry in the server. The names a
	// program uses are known when it is written, so checking them needs no state,
	// no round trip, and nothing that can disagree with itself later.
	Databases []string

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
	// SetDatabase selects a named database and returns the strategies over it.
	// It reaches the server before returning, so a bad address surfaces here
	// rather than at the first Get.
	//
	// The name is a namespace: every key this database reads and writes carries
	// it as a segment. That is deliberately the same on every backend — a name
	// isolates a cache from its neighbours identically on Redis, Dragonfly and
	// memcached, there is no registry to keep, no allocation to race over, and
	// no ceiling on how many you can have. It works on Redis Cluster, which has
	// only database 0 and would refuse any numeric selection but that one.
	//
	// What it does not give you is server-side isolation. Two names are kept
	// apart by everyone agreeing to use them, not by the server, so a FLUSHDB
	// still takes out every one of them and a key built by hand elsewhere can
	// still collide. Reach for [Provider.SelectIndex] when you want the server
	// to enforce the boundary.
	SetDatabase(ctx context.Context, name string) (*DB, error)

	// SelectIndex selects a database by index, where the backend has real ones.
	//
	// On a RESP server this is SELECT: the boundary is enforced by the server,
	// FLUSHDB reaches one database and not its neighbours. The costs are the
	// ones the server imposes — Redis ships with sixteen databases, Cluster has
	// only database 0, and an index other than the client's means a derived
	// client with a pool of its own, which the returned [DB] owns and closes.
	//
	// On a backend with no databases of its own the index becomes a key segment
	// instead, which is a weaker guarantee than the name suggests. Prefer
	// [Provider.SetDatabase] unless you specifically want SELECT.
	SelectIndex(ctx context.Context, index int) (*DB, error)

	// DropDatabase deletes every key belonging to a named database and reports
	// how many went.
	//
	// A namespace has no server-side existence, so this is a keyspace walk and a
	// delete rather than a FLUSHDB: it costs time proportional to the whole
	// keyspace, not to the database being dropped, and it is not atomic. Writes
	// arriving during the walk may survive it. That is the price of a database
	// the server does not know about, and it is why this is worth calling in a
	// teardown and worth thinking twice about anywhere else.
	//
	// Backends that cannot walk their keyspace report [ErrUnsupported].
	DropDatabase(ctx context.Context, name string) (int, error)

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

	// Name is the namespace this database was selected under, and is empty when
	// it was selected by index instead.
	Name string

	// Index reports which database index these strategies run against. After
	// [Provider.SetDatabase] that is whichever index the client was built on —
	// the name did not change it, and saying so is the point.
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
