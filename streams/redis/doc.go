// Package redis implements [streams.Streams] over Redis.
//
// You bring the connection:
//
//	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379", DB: 2})
//	defer rdb.Close()
//
//	s := redis.Connect(rdb)             // immediate, over pub/sub
//	n := redis.ConnectScheduled(rdb)    // on TTL expiry, over keyspace events
//	d := redis.ConnectDurable(rdb)      // durable, over Redis Streams
//
// This package never dials, never holds a connection in a package-level
// variable, and never closes the client it was handed.
//
// # Which constructor
//
// The first two hand a message over and forget it: a subscriber that is not
// attached when it is published never sees it, and one that dies holding it
// loses it. [ConnectDurable] appends to a log instead, tracks each named
// consumer's position on the server, and keeps a delivered message pending
// until it is acknowledged — so it is the only one of the three whose manager
// satisfies [streams.Durable] and [streams.Positioned].
//
// They are separate constructors rather than one with a flag because the
// difference is not a setting. A program that believes it has at-least-once
// delivery and actually has at-most-once loses messages under exactly the
// conditions it added the durability for.
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
