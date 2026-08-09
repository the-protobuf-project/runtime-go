// Package cache is the design spec for runtime-go's cache layer: the contracts,
// and nothing that implements them.
//
// It lives under arch/ but is named cache, so the example reads exactly as the
// shipping API will once these files move into cache/.
//
// # The flow
//
// Three steps, in this order:
//
//	client, err := redis.NewClient(redis.Config{Address: "localhost:6379"})
//	c := redis.New(client, cache.Config{Prefix: "orders"})
//	db, err := c.SetDatabase(ctx, 1)
//
// The client comes from the provider package rather than from a driver, so a
// program that caches never imports go-redis at all — which is what kept every
// import in it from needing an alias. It is also yours to keep: hand the same
// client to the database and streams layers and they share one pool.
//
// [Provider.SetDatabase] selects the database and hands back a [DB], and a DB is
// not one more cache interface but a set of named strategies over that database.
//
// # The strategies
//
// A cache is not one behavior. Storing a value you will later enumerate, a value
// you will only read back by key, and a value you want found by e-mail address
// are three jobs with three different costs, and one interface covering all of
// them either does the most expensive thing every time or grows options nobody
// can keep straight. So they are separate:
//
//   - [Document] — whole encoded values, enumerable, at the cost of an index.
//   - [Volatile] — TTL-first, no index, nothing to sweep, no enumeration.
//   - [Indexed] — Document plus lookups by a field other than the id.
//   - [Aside] — read-through over a loader, collapsing concurrent misses.
//
// # Written once
//
// None of the four is implemented per backend. The algorithms — sweeping a stale
// index, refiling an entry whose indexed field changed, collapsing a stampede
// into one load — are the same wherever the bytes land, so they live once in
// core and run on a driver: eight primitives a backend implements, plus a few
// capabilities it may not have.
//
// A new backend is that driver and a client. It writes no strategy code, and it
// cannot get the sweep subtly wrong in its own particular way, because it never
// sees the sweep.
//
// # What a backend cannot do
//
// Backends differ, and the differences are load-bearing. Memcache has no
// keyspace walk and no sets, so it cannot enumerate and cannot index. Where a
// capability is missing the strategy is still there and reports [ErrUnsupported]
// — a nil field would panic far from the wiring mistake that caused it, on a
// line that looks innocent.
package cache
