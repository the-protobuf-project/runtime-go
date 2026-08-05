// Package cache defines the backend-agnostic contract for runtime-go's cache
// layer: ephemeral, TTL-bound storage.
//
// Providers live in their own modules — [github.com/the-protobuf-project/runtime-go/redis]
// today, NATS and others alongside it — and reach this contract through a
// manager they hand back:
//
//	c, _ := redis.New(ctx, redis.Config{Address: "localhost", Port: "6379"})
//	mgr, _ := c.SetDatabase(ctx, "orders")
//
//	mgr.Document.Cache.Create(ctx, "", user, cache.TTL(time.Minute))
//
// # Your model, not ours
//
// There is no document or entry type here. A value goes in as it is and comes
// back decoded into a destination you own, so adding a field to your model is
// not a change to this package. [For] puts a typed view over any Cache when you
// want the compiler to check that:
//
//	users := cache.For[User](mgr.Document.Cache)
//	id, _ := users.Create(ctx, u, cache.TTL(time.Minute))
//	u2, _ := users.Get(ctx, id)
//
// The view is a wrapper, not a different client — one Cache serves every model,
// so a provider is configured once no matter how many types run through it.
//
// For durable records that never expire see the sibling
// [github.com/the-protobuf-project/runtime-go/database] module; for messaging,
// [github.com/the-protobuf-project/runtime-go/streams].
package cache
