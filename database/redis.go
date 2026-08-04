package database

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// RedisConfig wires the Redis store. Pass it to [Redis].
type RedisConfig struct {
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

// redisStore is the Redis implementation of [Store]. It is unexported because
// [Redis] hands back the interface — there is nothing useful to reach on the
// concrete type that the contract does not already cover.
type redisStore struct {
	rdb  goredis.UniversalClient
	keys redisKeys
}

var _ Store = (*redisStore)(nil)

// Redis opens a Redis-backed document store with content-addressed
// deduplication.
//
// The caller supplies the client:
//
//	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379", DB: 0})
//	defer rdb.Close()
//
//	db, err := database.Redis(database.RedisConfig{Client: rdb, Prefix: "orders"})
//
// This never dials, never caches a connection in a package-level variable, and
// never closes the client it was handed.
//
// It fails if no client was supplied, rather than returning a value whose every
// method would nil-panic on first use. It does not ping the server — see
// [PingRedis] for an explicit readiness check.
func Redis(cfg RedisConfig) (Store, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("database: RedisConfig.Client is required")
	}
	return &redisStore{
		rdb:  cfg.Client,
		keys: newRedisKeys(cfg.Prefix),
	}, nil
}

// PingRedis verifies a store built by [Redis] can reach its server. It is
// offered because [Redis] deliberately does not dial, so a caller that wants a
// readiness check has somewhere to make one.
//
// It reports an error for a store that did not come from [Redis].
func PingRedis(ctx context.Context, s Store) error {
	rs, ok := s.(*redisStore)
	if !ok {
		return fmt.Errorf("database: PingRedis needs a store built by Redis, got %T", s)
	}
	if err := rs.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("database: redis ping failed: %w", err)
	}
	return nil
}
