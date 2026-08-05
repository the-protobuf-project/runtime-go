// Package redis implements runtime-go's cache, database, and streams contracts
// over Redis.
//
// One client serves all three. Open it, name a logical database, and reach the
// operations through the manager that database hands back:
//
//	c, _ := redis.New(ctx, redis.Config{Address: "localhost", Port: "6379"})
//	defer c.Close()
//
//	_ = c.CreateDatabase(ctx, "orders")
//	mgr, _ := c.SetDatabase(ctx, "orders")
//
//	mgr.Document.Cache.Create(ctx, "", order, cache.TTL(time.Minute))
//	mgr.Document.KV.Create(ctx, "", order)
//	mgr.Channel.Stream.Create(ctx, streams.Stream{ … })
//
// # Named databases
//
// Redis numbers its databases 0–15. Naming them keeps that numbering out of
// application code: the name→index mapping lives in database 0, and 1 is
// reserved, so the first name you create lands on 2.
//
// [Client.SetDatabase] opens a connection bound to that index and returns a
// manager over it. It does not re-point the client it was called on, so
// managers for different databases coexist and an earlier one keeps working
// after a later one is made.
//
// # Your model, not ours
//
// Nothing here defines a document type. Values are stored as you hand them over
// and decoded into a destination you own, so a model gaining a field is not a
// change to this package. Use the typed views — cache.For, database.For,
// streams.For — when you want the compiler to check the shape.
package redis
