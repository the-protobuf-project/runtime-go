// Package redis implements the [cache.Cache] contract over Redis, storing
// entries as JSON strings with a native Redis TTL.
//
// The caller supplies the client:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 2})
//	defer rdb.Close()
//
//	c, err := cacheredis.New(cacheredis.Config{Client: rdb, Prefix: "orders"})
//
// This package never dials, never caches a connection in a package-level
// variable, and never closes the client it was handed — pooling, the database
// index, TLS, and shutdown all stay with the caller, and two caches built from
// two different clients are genuinely independent.
package redis

import (
	"fmt"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/cache"
)

// Config wires a [Cache].
type Config struct {
	// Client is the Redis client to use. Required.
	//
	// It is a UniversalClient so a single, cluster, or failover client all work
	// unchanged. The caller owns its lifetime and is responsible for closing it.
	Client goredis.UniversalClient

	// Prefix namespaces every key this cache reads and writes. Optional.
	//
	// Use it to run several independent caches against one Redis database, or
	// to share a database with another concern. Entries written under one
	// prefix are invisible to a cache configured with another.
	Prefix string
}

// Cache is a Redis-backed [cache.Cache].
type Cache struct {
	rdb  goredis.UniversalClient
	keys keys
}

// Cache implements the module-wide contract.
var _ cache.Cache = (*Cache)(nil)

// New builds a Cache from cfg. It fails if no client was supplied, rather than
// returning a value whose every method would nil-panic on first use.
//
// It does not ping the server: the client is the caller's, they may have
// already checked it, and a constructor that reaches the network cannot be
// used to build a cache before the server is up.
func New(cfg Config) (*Cache, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("cache/redis: Config.Client is required")
	}
	return &Cache{
		rdb:  cfg.Client,
		keys: newKeys(cfg.Prefix),
	}, nil
}
