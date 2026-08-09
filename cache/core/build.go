package core

import (
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/ulid"
)

// Timings core picks rather than asking a caller to. Each bounds something that
// would otherwise be unbounded, and none of them changes what an operation
// means — only how long it can go wrong for.
const (
	loadTimeout    = 30 * time.Second // one loader run, detached from any request
	lockLease      = 30 * time.Second // a cross-process lock, if a dead holder leaves one
	voidFor        = 30 * time.Second // how long an absence is remembered
	drainTimeout   = 5 * time.Second  // how long Close waits for background refreshes
	refreshBudget  = 64               // concurrent background refreshes per database
	flightCapacity = 30 * time.Second // how long a joined load may be waited on
)

// Spec is what a provider knows and core does not: which database was selected,
// how its keys should be namespaced, and what to release when it closes.
type Spec struct {
	// Prefix namespaces every key.
	Prefix string

	// Namespace is the name the database was selected under, and is empty when
	// it was selected by index. It becomes a key segment on every backend, which
	// is what makes a named database mean the same thing on all of them.
	Namespace string

	// Database is the index that was selected.
	Database int

	// EmbedDB puts that index into the keys, for a backend that has no databases
	// of its own and has to fake them with a namespace.
	EmbedDB bool

	// DefaultTTL is the lease applied when an operation names none.
	DefaultTTL time.Duration

	// DefaultStale is the stale-serving window applied when an operation names
	// none. Zero keeps read-through blocking on expiry.
	DefaultStale time.Duration

	// Concurrency caps the fan-out when one operation covers many keys. Zero
	// takes [defaultConcurrency].
	Concurrency int

	// RequireTTL makes a write that resolved to no expiry an error. See
	// [cache.Config.RequireTTL].
	RequireTTL bool

	// NewID generates an id when a caller does not supply one. Defaults to
	// [ulid.Generate], which is sortable by creation time and unique across
	// processes — the second property mattering the moment two of them write to
	// one cache.
	NewID func() string

	// Release frees what selecting the database allocated, or is nil when
	// selecting it allocated nothing. Core wraps it — see [Build].
	Release func() error
}

// Build wires a driver into the four strategies.
//
// This is the whole of what a backend gets for implementing [Driver]. The
// capabilities are resolved once, here, rather than per call: a driver cannot
// grow sets halfway through a request, and a type assertion on every operation
// would be a cost paid forever for a fact known at construction.
func Build(driver Driver, spec Spec) *cache.DB {
	sets, _ := driver.(Sets)
	leases, _ := driver.(Leases)
	scanner, _ := driver.(Scanner)
	bulk, _ := driver.(Bulk)
	fenced, _ := driver.(Fenced)

	newID := spec.NewID
	if newID == nil {
		newID = nextID
	}
	limit := spec.Concurrency
	if limit < 1 {
		limit = defaultConcurrency
	}

	keys := NewKeyspace(spec.Prefix, spec.Namespace, spec.Database, spec.EmbedDB)
	def := cache.Options{TTL: spec.DefaultTTL, Stale: spec.DefaultStale}

	// One flight and one refresh budget per database, shared by every read-through
	// view over it: two callers loading the same id through different loaders are
	// still loading the same id, and the budget is a property of the connection
	// rather than of any one view.
	flights := newFlight(flightCapacity)
	fresh := newRefresher(refreshBudget, loadTimeout)

	entries := func(segment string) *document {
		return &document{
			driver: driver, sets: sets, leases: leases, bulk: bulk,
			keys: keys.Strategy(segment), def: def, limit: limit, newID: newID,
			require: spec.RequireTTL,
		}
	}

	return &cache.DB{
		Document: entries("doc"),
		Volatile: &volatile{
			driver: driver, leases: leases, scanner: scanner,
			keys: keys.Strategy("vol"), def: def, require: spec.RequireTTL,
		},
		Indexed: &indexed{document: entries("idx")},
		Backend: driver.Name(),
		Name:    spec.Namespace,
		Index:   spec.Database,
		Release: release(fresh, spec.Release),
		Aside: func(load cache.Loader) cache.Aside {
			return &aside{
				driver:  driver,
				fenced:  fenced,
				keys:    keys.Strategy("aside"),
				load:    load,
				def:     def,
				flight:  flights,
				fresh:   fresh,
				require: spec.RequireTTL,
				empty:   voidFor,
				lease:   lockLease,
			}
		},
	}
}

// release drains background work before freeing what it was running against.
//
// The order is the point. Closing a derived client while a refresh is still
// using it turns a clean shutdown into a handful of errors from goroutines
// nobody is listening to, and both failures are reported rather than the first
// one silently winning.
func release(fresh *refresher, next func() error) func() error {
	return func() error {
		drained := fresh.Drain(drainTimeout)
		if next != nil {
			if err := next(); err != nil {
				return err
			}
		}
		return drained
	}
}

// nextID is the default id generator, shared with the rest of runtime-go so an
// id from the cache looks like an id from anywhere else.
func nextID() string { return ulid.Generate().GetRandomCode() }
