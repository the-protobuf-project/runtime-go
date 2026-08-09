// Package redis is the Redis preset.
//
// It is a thin layer over [resp], which drives every RESP server this module
// supports. Redis contributes no primitives of its own — there is nothing here
// but a name and the defaults that go with it, which is the point: Dragonfly
// next door is the same file with different constants.
//
//	client, err := redis.NewClient(ctx, redis.Config{Address: "localhost:6379"})
//	defer client.Close()
//
//	c := redis.New(client, cache.Config{Prefix: "orders"})
//	db, err := c.SetDatabase(ctx, 1)
//	defer db.Close()
//
// The client is yours: this package will not close it. Hand the same one to the
// database and streams layers and all three share a pool.
package redis
