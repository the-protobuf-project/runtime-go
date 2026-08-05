// Package redis implements [streams.Streams] over Redis.
//
// You bring the connection:
//
//	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379", DB: 2})
//	defer rdb.Close()
//
//	s := redis.Connect(rdb)             // immediate, over pub/sub
//	n := redis.ConnectScheduled(rdb)    // on TTL expiry, over keyspace events
//
// This package never dials, never holds a connection in a package-level
// variable, and never closes the client it was handed.
//
// # Swapping providers
//
// Connect returns the [streams.Streams] interface, so the connection object is
// the only thing that changes when you move to another backend:
//
//	s := redis.Connect(rdb)          // Redis
//	s := nats.Connect(nc)            // NATS — same interface, same calls
//
// # Scheduled delivery needs a server flag
//
// [ConnectScheduled] rides on Redis keyspace notifications, so the server must
// run with `--notify-keyspace-events Ex`. Without it, published messages simply
// never fire.
package redis
