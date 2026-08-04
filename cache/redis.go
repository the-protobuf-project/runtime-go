package cache

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// RedisConfig wires the Redis cache. Pass it to [Redis].
type RedisConfig struct {
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

	// Logger receives detail the [Cache] contract cannot express — which key a
	// resource name was masked to, which stale index members were swept.
	// Optional; defaults to [telemetry.NoopLogger].
	//
	// This is for the driver's internals. For a record per operation, wrap the
	// cache with [WithLogging] instead; the two compose.
	Logger telemetry.Logger
}

// redisCache is the Redis implementation of [Cache]. It is unexported because
// [Redis] hands back the interface — there is nothing useful to reach on the
// concrete type that the contract does not already cover.
type redisCache struct {
	rdb  goredis.UniversalClient
	keys redisKeys
	log  telemetry.Logger
}

var _ Cache = (*redisCache)(nil)

// Redis opens a Redis-backed cache, storing entries as JSON strings with a
// native Redis TTL.
//
// The caller supplies the client:
//
//	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379", DB: 1})
//	defer rdb.Close()
//
//	c, err := cache.Redis(cache.RedisConfig{Client: rdb, Prefix: "orders"})
//
// This never dials, never caches a connection in a package-level variable, and
// never closes the client it was handed — pooling, the database index, TLS, and
// shutdown all stay with the caller, and two caches built from two different
// clients are genuinely independent.
//
// It fails if no client was supplied, rather than returning a value whose every
// method would nil-panic on first use. It does not ping the server: the client
// is the caller's, they may have already checked it, and a constructor that
// reaches the network cannot be used to build a cache before the server is up —
// see [PingRedis] for an explicit readiness check.
func Redis(cfg RedisConfig) (Cache, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("cache: RedisConfig.Client is required")
	}
	log := cfg.Logger
	if log == nil {
		log = telemetry.NoopLogger
	}
	log.Info(context.Background(), "cache opened", telemetry.Fields{
		"backend": "redis",
		"prefix":  cfg.Prefix,
	})
	return &redisCache{
		rdb:  cfg.Client,
		keys: newRedisKeys(cfg.Prefix),
		log:  log,
	}, nil
}

// PingRedis verifies a cache built by [Redis] can reach its server. It is
// offered because [Redis] deliberately does not dial, so a caller that wants a
// readiness check has somewhere to make one.
//
// It reports an error for a cache that did not come from [Redis].
func PingRedis(ctx context.Context, c Cache) error {
	rc, ok := c.(*redisCache)
	if !ok {
		return fmt.Errorf("cache: PingRedis needs a cache built by Redis, got %T", c)
	}
	if err := rc.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("cache: redis ping failed: %w", err)
	}
	return nil
}
