package redis

import (
	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Option configures a [Connect] or [ConnectScheduled].
type Option func(*config)

type config struct {
	prefix string
	db     int
	log    telemetry.Logger
	meter  telemetry.Meter
}

// WithPrefix namespaces every key these streams read and write.
//
// Use it to run several independent sets of streams against one Redis database,
// or to share a database with another concern.
func WithPrefix(prefix string) Option {
	return func(c *config) { c.prefix = prefix }
}

// WithDatabase tells [ConnectScheduled] which database index to watch for
// expiry events.
//
// Keyspace notifications are published per database as
// `__keyevent@<db>__:expired`, so scheduled delivery only works when this
// matches the client's database. It is read from the client automatically for a
// *redis.Client; set it explicitly for a cluster or failover client, whose
// database cannot be inspected.
func WithDatabase(index int) Option {
	return func(c *config) { c.db = index }
}

// WithLogger sets where these streams write their own records — which channel a
// subject resolved to, which expiry events were filtered out. Defaults to
// [telemetry.NoopLogger].
func WithLogger(log telemetry.Logger) Option {
	return func(c *config) { c.log = log }
}

// WithMeter sets where these streams report their own measurements. Defaults to
// [telemetry.NoopMeter].
func WithMeter(m telemetry.Meter) Option {
	return func(c *config) { c.meter = m }
}

// Connect returns a [streams.Streams] backed by rdb, delivering immediately
// over pub/sub.
//
// The client is yours: this package does not dial it, does not close it, and
// does not cache it anywhere.
func Connect(rdb goredis.UniversalClient, opts ...Option) streams.Streams {
	return connect(rdb, kindStream, opts...)
}

// ConnectScheduled returns a [streams.Streams] backed by rdb that delivers a
// message when its TTL expires rather than when it is published — a reminder, a
// lease timeout, a delayed retry.
//
// It is a separate constructor rather than an option because the two behave
// differently enough to be worth naming: a scheduled publish requires a TTL and
// an immediate one rejects it. Streams created through it live in their own key
// namespace, so they never appear in an immediate [Connect]'s List.
//
// This relies on Redis keyspace notifications, so the server must run with
// `--notify-keyspace-events Ex`; without it, scheduled messages never fire.
func ConnectScheduled(rdb goredis.UniversalClient, opts ...Option) streams.Streams {
	return connect(rdb, kindNotify, opts...)
}

func connect(rdb goredis.UniversalClient, k kind, opts ...Option) streams.Streams {
	cfg := config{log: telemetry.NoopLogger, meter: telemetry.NoopMeter}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.log == nil {
		cfg.log = telemetry.NoopLogger
	}
	if cfg.meter == nil {
		cfg.meter = telemetry.NoopMeter
	}

	// A plain client knows its own database; a cluster or failover client does
	// not, which is why WithDatabase exists.
	db := cfg.db
	if db == 0 {
		if c, ok := rdb.(*goredis.Client); ok {
			db = c.Options().DB
		}
	}

	return &streamHandler{
		rdb:  rdb,
		keys: newKeys(cfg.prefix, k),
		kind: k,
		db:   db,
		log:  cfg.log,
	}
}
