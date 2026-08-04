// Package redis implements the [streams.Streams] contract over Redis,
// delivering ordinary messages through pub/sub and expiry notifications through
// keyspace events.
//
// The caller supplies the client:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 0})
//	defer rdb.Close()
//
//	s, err := streamsredis.New(streamsredis.Config{Client: rdb, Prefix: "app"})
//
// This package never dials, never caches a connection in a package-level
// variable, and never closes the client it was handed.
//
// # Notifications need a server flag
//
// [Provider.Notifications] delivers when a message's TTL expires, which relies
// on Redis keyspace notifications. The server must run with
// `--notify-keyspace-events Ex`; without it, published notifications simply
// never fire. The docker/compose.yaml in this module sets it.
package redis

import (
	"context"
	"fmt"
	"strconv"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/streams"
)

// itoa is strconv.Itoa under a shorter name, used by the key helpers.
func itoa(i int) string { return strconv.Itoa(i) }

// Config wires a [Provider].
type Config struct {
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

// Provider is a Redis-backed [streams.Streams].
type Provider struct {
	rdb  goredis.UniversalClient
	keys keys
	db   int

	// notify is the sibling provider for expiry-driven delivery. It shares the
	// client but uses its own key namespace, so a notification stream never
	// shows up in an ordinary List.
	notify *Provider
}

// Provider implements the module-wide contracts.
var (
	_ streams.Streams  = (*Provider)(nil)
	_ streams.Notifier = (*Provider)(nil)
)

// New builds a Provider from cfg. It fails if no client was supplied, rather
// than returning a value whose every method would nil-panic on first use.
//
// It does not ping the server: the client is the caller's, and a constructor
// that reaches the network cannot be used to build a provider before the
// server is up.
func New(cfg Config) (*Provider, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("streams/redis: Config.Client is required")
	}

	db := cfg.DB
	if db == 0 {
		// A plain client knows its own database; a cluster or failover client
		// does not, which is why Config.DB exists.
		if c, ok := cfg.Client.(*goredis.Client); ok {
			db = c.Options().DB
		}
	}

	p := &Provider{
		rdb:  cfg.Client,
		keys: newKeys(cfg.Prefix, "stream"),
		db:   db,
	}
	p.notify = &Provider{
		rdb:  cfg.Client,
		keys: newKeys(cfg.Prefix, "notify"),
		db:   db,
	}
	return p, nil
}

// Notifications returns the lifecycle interface for expiry-driven channels,
// satisfying [streams.Notifier].
//
// Streams created through it live in their own key namespace, so they never
// appear in this provider's List and vice versa.
func (p *Provider) Notifications() streams.Streams {
	return p.notify
}

// isNotify reports whether this provider delivers on expiry rather than
// immediately.
func (p *Provider) isNotify() bool { return p.keys.kind == "notify" }

// Ping verifies the provider can reach its server. It is offered because New
// deliberately does not dial, so a caller that wants a readiness check has
// somewhere to make one.
func (p *Provider) Ping(ctx context.Context) error {
	if err := p.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("streams/redis: ping failed: %w", err)
	}
	return nil
}
