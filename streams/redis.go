package streams

import (
	"context"
	"fmt"
	"strconv"

	goredis "github.com/redis/go-redis/v9"
)

// itoa is strconv.Itoa under a shorter name, used by the key helpers.
func itoa(i int) string { return strconv.Itoa(i) }

// RedisStreams is what [Redis] returns: the lifecycle contract plus the
// expiry-driven notification channels Redis can also serve.
//
// It is a distinct interface rather than a bare [Streams] because a provider
// that cannot schedule delivery — a plain broker — should not have to pretend
// it can. Assign it to a [Streams] wherever notifications are not needed.
type RedisStreams interface {
	Streams
	Notifier
}

// RedisConfig wires the Redis provider. Pass it to [Redis].
type RedisConfig struct {
	// Client is the Redis client to use. Required.
	//
	// It is a UniversalClient so a single, cluster, or failover client all work
	// unchanged. The caller owns its lifetime and is responsible for closing it.
	Client goredis.UniversalClient

	// Prefix namespaces every key this provider reads and writes. Optional.
	//
	// Use it to run several independent sets of streams against one Redis
	// database, or to share a database with another concern.
	Prefix string

	// DB is the database index used to build the keyspace-notification channel
	// for expiry delivery. Optional.
	//
	// Keyspace events are published per database as `__keyevent@<db>__:expired`,
	// so notifications only work if this matches the client's database. It is
	// read from the client automatically for a *redis.Client; set it explicitly
	// when passing a cluster or failover client, whose database cannot be
	// inspected.
	DB int
}

// redisProvider is the Redis implementation of [Streams]. It is unexported
// because [Redis] hands back the interface.
type redisProvider struct {
	rdb  goredis.UniversalClient
	keys redisKeys
	db   int

	// notify is the sibling provider for expiry-driven delivery. It shares the
	// client but uses its own key namespace, so a notification stream never
	// shows up in an ordinary List.
	notify *redisProvider
}

var (
	_ Streams  = (*redisProvider)(nil)
	_ Notifier = (*redisProvider)(nil)
)

// Redis opens a Redis-backed streams provider, delivering ordinary messages
// through pub/sub and expiry notifications through keyspace events.
//
// The caller supplies the client:
//
//	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379", DB: 0})
//	defer rdb.Close()
//
//	s, err := streams.Redis(streams.RedisConfig{Client: rdb, Prefix: "app"})
//
// This never dials, never caches a connection in a package-level variable, and
// never closes the client it was handed.
//
// Expiry notifications need the server running with `--notify-keyspace-events
// Ex`; without it, published notifications simply never fire.
//
// It fails if no client was supplied, rather than returning a value whose every
// method would nil-panic on first use. It does not ping the server — see
// [PingRedis] for an explicit readiness check.
func Redis(cfg RedisConfig) (RedisStreams, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("streams: Config.Client is required")
	}

	db := cfg.DB
	if db == 0 {
		// A plain client knows its own database; a cluster or failover client
		// does not, which is why Config.DB exists.
		if c, ok := cfg.Client.(*goredis.Client); ok {
			db = c.Options().DB
		}
	}

	p := &redisProvider{
		rdb:  cfg.Client,
		keys: newRedisKeys(cfg.Prefix, "stream"),
		db:   db,
	}
	p.notify = &redisProvider{
		rdb:  cfg.Client,
		keys: newRedisKeys(cfg.Prefix, "notify"),
		db:   db,
	}
	return p, nil
}

// Notifications returns the lifecycle interface for expiry-driven channels,
// satisfying [Notifier].
//
// Streams created through it live in their own key namespace, so they never
// appear in this provider's List and vice versa.
func (p *redisProvider) Notifications() Streams {
	return p.notify
}

// isNotify reports whether this provider delivers on expiry rather than
// immediately.
func (p *redisProvider) isNotify() bool { return p.keys.kind == "notify" }

// PingRedis verifies a provider built by [Redis] can reach its server. It is
// offered because [Redis] deliberately does not dial, so a caller that wants a
// readiness check has somewhere to make one.
//
// It reports an error for a provider that did not come from [Redis].
func PingRedis(ctx context.Context, s Streams) error {
	rp, ok := s.(*redisProvider)
	if !ok {
		return fmt.Errorf("streams: PingRedis needs a provider built by Redis, got %T", s)
	}
	if err := rp.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("streams: redis ping failed: %w", err)
	}
	return nil
}
