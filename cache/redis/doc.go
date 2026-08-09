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
//	c := redis.New(client, cache.Config{Prefix: "example"})
//	db, err := c.SetDatabase(ctx, "orders")
//	defer db.Close()
//
// The name namespaces the keys and leaves the connection on whichever database
// the client was built for. Use [cache.Provider.SelectIndex] instead when you
// want Redis to enforce the boundary with SELECT — worth knowing that Redis
// ships with sixteen databases and a cluster has only database 0.
//
// The client is yours: this package will not close it. Hand the same one to the
// database and streams layers and all three share a pool.
package redis
