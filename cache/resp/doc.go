// Package resp is the driver for anything speaking the Redis serialization
// protocol: Redis itself, Dragonfly, Valkey, KeyDB.
//
// It exists because those servers differ in their internals and not in the eight
// primitives a cache needs from them. One driver, one client, one place where a
// pipeline or a Lua script is written — and a new RESP backend is a preset that
// names itself and picks defaults, rather than a copy of this with a different
// word in the error messages.
//
// Most callers want a preset instead:
//
//	client, err := redis.NewClient(ctx, redis.Config{Address: "localhost:6379"})
//	client, err := dragonfly.NewClient(ctx, dragonfly.Config{Address: "localhost:6379"})
//
// Reach for this package directly when you already hold a driver client and want
// it adopted as it is — see [Wrap] — or when your server speaks RESP and has no
// preset here yet, in which case [Config.Backend] is the name it will report.
//
// # Capabilities
//
// A RESP server has all five: sets, a readable TTL, a keyspace cursor, pipelined
// bulk reads, and scripted compare-and-delete. So every strategy works and none
// of them reports cache.ErrUnsupported. That is unusual — see the memcached
// package for the other case, and for why the capability split earns its keep.
package resp
