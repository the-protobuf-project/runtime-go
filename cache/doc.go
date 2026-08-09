// Package cache is the contract for runtime-go's cache layer: the interfaces,
// and nothing that implements them.
//
// # The flow
//
// Three steps, in this order:
//
//	client, err := redis.NewClient(ctx, redis.Config{Address: "localhost:6379"})
//	c := redis.New(client, cache.Config{Prefix: "example"})
//	db, err := c.SetDatabase(ctx, "orders")
//
// The client comes from the provider package rather than from a driver, so a
// program that caches never imports go-redis at all — which is what kept every
// import in it from needing an alias. It is also yours to keep: hand the same
// client to the database and streams layers and they share one pool.
//
// [Provider.SetDatabase] selects the database and hands back a [DB], and a DB is
// not one more cache interface but a set of named strategies over that database.
//
// # Choosing a database
//
// A name is a namespace: it qualifies every key the database touches and leaves
// the connection alone. That makes it the one selection form whose meaning does
// not change underneath a program — orders means the same thing on Redis,
// Dragonfly and memcached, there is no registry to keep or allocation to race
// over, no ceiling on how many you can have, and it works on Redis Cluster,
// which has only database 0.
//
// What a name does not do is make the server enforce the boundary. Two names
// are kept apart by everyone agreeing to use them: a FLUSHDB reaches both, and a
// key built by hand elsewhere can still collide. [Provider.SelectIndex] is the
// other trade — real SELECT on a RESP server, at the cost of the server's
// limits (sixteen databases by default, database 0 only on a cluster) and of a
// derived client when the index is not the one you built.
//
//	db, err := c.SetDatabase(ctx, "orders")  // portable, no server support needed
//	db, err := c.SelectIndex(ctx, 3)         // real SELECT, where the backend has it
//
// # Expiry
//
// [Config.DefaultTTL] is the lease for every entry, and any one operation
// overrides it with [TTL]. That is the whole of it for most callers: set it
// once, name it again only where something should live longer or shorter.
//
// A zero TTL means no expiry. For a cache that is rarely what silence was meant
// to say, and for [Aside] it is a leak — a read-through cache with no lease
// keeps every id it was ever asked for, and the trigger is any client requesting
// distinct ids. [Config.RequireTTL] turns that silence into an error at the
// first write, while leaving a deliberate permanent entry alone: say [NoExpiry]
// and it is allowed through.
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
