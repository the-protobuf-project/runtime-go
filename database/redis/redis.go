// Package redis implements the [database.Store] contract over Redis, storing
// documents as canonical JSON with content-addressed deduplication.
//
// The caller supplies the client:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 0})
//	defer rdb.Close()
//
//	db, err := dbredis.New(dbredis.Config{Client: rdb, Prefix: "orders"})
//
// This package never dials, never caches a connection in a package-level
// variable, and never closes the client it was handed.
//
// # Deduplication
//
// Every document body is canonicalized (map keys sorted) and hashed with
// SHA256, and the hash is reserved atomically before the body is written.
// Creating a document whose content already exists returns the existing
// document rather than storing a second copy, so identical content always
// resolves to one ID.
package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/database"
)

// Config wires a [Store].
type Config struct {
	// Client is the Redis client to use. Required.
	//
	// It is a UniversalClient so a single, cluster, or failover client all work
	// unchanged. The caller owns its lifetime and is responsible for closing it.
	Client goredis.UniversalClient

	// Prefix namespaces every key this store reads and writes. Optional.
	//
	// Use it to run several independent stores against one Redis database, or
	// to share a database with another concern — a cache, for instance.
	Prefix string
}

// Store is a Redis-backed [database.Store].
type Store struct {
	rdb  goredis.UniversalClient
	keys keys
}

// Store implements the module-wide contract.
var _ database.Store = (*Store)(nil)

// New builds a Store from cfg. It fails if no client was supplied, rather than
// returning a value whose every method would nil-panic on first use.
//
// It does not ping the server: the client is the caller's, and a constructor
// that reaches the network cannot be used to build a store before the server
// is up.
func New(cfg Config) (*Store, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("database/redis: Config.Client is required")
	}
	return &Store{
		rdb:  cfg.Client,
		keys: newKeys(cfg.Prefix),
	}, nil
}

// Ping verifies the store can reach its server. It is offered because New
// deliberately does not dial, so a caller that wants a readiness check has
// somewhere to make one.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("database/redis: ping failed: %w", err)
	}
	return nil
}
