// Package dragonfly is the Dragonfly preset.
//
// Dragonfly speaks the Redis protocol, so it runs on the same [resp] driver as
// Redis and every strategy works unchanged: sets, a readable TTL, a keyspace
// cursor, pipelined bulk reads, and the scripted compare-and-delete that fences
// the read-through lock. Supporting it out of the box costs this file.
//
//	client, err := dragonfly.NewClient(ctx, dragonfly.Config{Address: "localhost:6379"})
//	defer client.Close()
//
//	c := dragonfly.New(client, cache.Config{Prefix: "example"})
//	db, err := c.SetDatabase(ctx, "orders")
//
// # What is different
//
// Two things worth knowing, neither of which changes an API here.
//
// Databases are configurable rather than fixed. Dragonfly serves a number of
// them set at startup with --dbnum, defaulting to 16, and selecting one past
// that is refused by the server. That is a limit on
// [cache.Provider.SelectIndex] and not on [cache.Provider.SetDatabase], which
// namespaces keys and never leaves the connection's own database. Both ping
// before handing anything back, so a refusal arrives as a startup error naming
// what was asked for rather than as a failed write later.
//
// Cluster mode is emulated. A single Dragonfly node reports itself as a cluster
// of one when asked, and the driver treats it as an ordinary client, which is
// what it is. Multi-node Dragonfly is addressed like a Redis cluster and carries
// the same restriction: database 0 only.
package dragonfly
