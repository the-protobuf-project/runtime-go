// Package redis implements [database.Store] over Redis, storing records as
// canonical JSON with content-addressed deduplication.
//
// You bring the connection:
//
//	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379", DB: 2})
//	defer rdb.Close()
//
//	db := redis.Connect(rdb)
//
// This package never dials, never holds a connection in a package-level
// variable, and never closes the client it was handed.
//
// # Swapping providers
//
// Connect returns the [database.Store] interface, so the connection object is
// the only thing that changes when you move to another backend:
//
//	db := redis.Connect(rdb)         // Redis
//	db := mongodb.Connect(client)    // MongoDB — same interface, same calls
//
// # Deduplication
//
// Every value is canonicalized — object keys sorted — and hashed with SHA256,
// and the hash is reserved atomically before the value is written. Creating a
// record whose content already exists returns the existing id rather than
// storing a second copy, so identical content always resolves to one record.
package redis
