// Package cached puts a read-through cache in front of any store.Driver, so a
// record read repeatedly is fetched from the backing store once.
//
// It is a decorator, not a backend: [Wrap] takes a [store.DB] and returns one,
// so the gRPC adapter, the typed views and every existing call site keep
// working, and the database underneath keeps its capabilities.
//
//	client, _ := rediscache.NewClient(ctx, rediscache.Config{Address: "localhost:6379"})
//	cdb, _ := rediscache.New(client, cache.Config{DefaultTTL: 5 * time.Minute}).
//	    SetDatabase(ctx, "records")
//
//	db, _ := orm.NewProvider(gormDB).SetDatabase(ctx, "tenant_a")
//	db = cached.Wrap(db, cached.FromAside(cdb))
//
//	db.Get(ctx, res, "books/dune")   // Redis first, Postgres on a miss
//
// # What it is for
//
// The read path, and only the read path. Concurrent misses on one key collapse
// into a single load, so a hot record expiring under traffic costs the backing
// store one query rather than one per caller; an absence is remembered, so
// requests for a record that does not exist stop reaching it at all. Those two
// properties are why this exists — see [Driver] for what is deliberately not
// cached.
//
// # Correctness
//
// Writes go to the backing store first and drop the cached copy after, so a
// record is never served from the cache after a write it did not observe,
// except in the window between those two steps. A transaction is handled the
// same way but deferred: reads inside one go straight to the backing store,
// because a transaction has to see its own writes, and what it wrote is dropped
// from the cache once it commits — and left alone when it rolls back, where the
// cache is still right.
//
// Records travel through the cache as proto wire format carried in a []byte.
// That is not a detail: a cache encodes what it is handed, generally as JSON,
// and JSON cannot carry a proto message at all — while a []byte survives it
// exactly, as base64, at about a third more space. Anything else silently
// corrupts a bytes field or a large integer.
//
// # A TTL is not optional
//
// Every write invalidates, so entries do not normally go stale. The TTL covers
// what falls outside that: an invalidation that fails after its write
// succeeded, a schema dropped underneath, another process writing to the same
// database without this decorator in front of it. Without one, a wrong entry
// stays wrong until something happens to overwrite it, and nothing guarantees
// anything will.
package cached
