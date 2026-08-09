// Package database defines the backend-agnostic contract for runtime-go's
// document-store layer: durable records that live until they are deleted.
//
// Providers live in subpackages and take a client you own, so nothing here dials
// or closes a connection:
//
//	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
//	store := redis.Connect(rdb, redis.WithPrefix("orders"))
//
//	id, _ := store.Create(ctx, "", book)
//
// The prefix is how several independent stores share one database, or share it
// with a cache or a stream. Hand the same client to all three and they share a
// pool.
//
// # Your model, not ours
//
// There is no document type here. A value goes in as it is and comes back
// decoded into a destination you own, so adding a field to your model is not a
// change to this package. [For] puts a typed view over any Store when you want
// the compiler to check that:
//
//	books := database.For[Book](store)
//	id, _ := books.Create(ctx, b)
//	b2, _ := books.Get(ctx, id)
//
// # Not a cache
//
// Records here have no TTL and do not expire. For ephemeral, TTL-bound entries
// use the sibling [github.com/the-protobuf-project/runtime-go/cache] module.
// The two are separate because a cache miss is routine and a missing record is
// not.
//
// # Not the proto Driver
//
// This is a store for ad-hoc values. The generated-proto CRUD seam —
// [github.com/the-protobuf-project/runtime-go/interfaces/store] — operates on
// proto.Message values through Resource descriptors and serves a different job.
package database
