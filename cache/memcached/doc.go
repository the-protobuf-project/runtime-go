// Package memcached is the memcached backend, and the argument for the driver
// split.
//
// It implements the eight required primitives and one of the four optional
// capabilities, because memcached has no sets, no keyspace walk, no way to
// report a remaining lease, and no conditional delete. That single fact — a type
// not implementing four interfaces — is the whole of what makes Document refuse
// to enumerate, Indexed refuse every lookup, TTL say why, and read-through fall
// back to collapsing loads within a process instead of across them. Not one line
// of strategy code was written, skipped, or special-cased to get there.
//
//	client, err := memcached.NewClient(memcached.Config{Servers: []string{"localhost:11211"}})
//	defer client.Close()
//
//	c := memcached.New(client, cache.Config{Prefix: "orders"})
//	db, err := c.SetDatabase(ctx, 3)
//
// Volatile works fully, which is unsurprising — leased values fetched by a key
// the caller already has is what memcached is. Aside works fully too, which is
// less obvious and more useful: read-through needs only a get, a lease, and an
// atomic add, so the strategy callers reach for most is the one that cares least
// about which backend is underneath.
//
// # Naming
//
// The server is memcached and the Go client for it is package memcache, so this
// package takes the server's name and the client keeps its own. Nothing here is
// imported under an alias, and nothing needs to be.
package memcached
