// Package redis implements [cache.Cache] over Redis.
//
// You bring the connection:
//
//	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379", DB: 2})
//	defer rdb.Close()
//
//	c := redis.Connect(rdb)
//
// This package never dials, never holds a connection in a package-level
// variable, and never closes the client it was handed. Pooling, TLS, the
// database index, and shutdown all stay with you.
//
// # Swapping providers
//
// Connect returns the [cache.Cache] interface, so the connection object is the
// only thing that changes when you move to another backend:
//
//	c := redis.Connect(rdb)          // Redis
//	c := memcached.Connect(mc)       // Memcached — same interface, same calls
//
// Nothing downstream of Connect knows which one it got. That is also why
// Redis's numbered databases are not part of this API: Memcached has none, so
// selecting one belongs on the client you build, not on a method here.
package redis
