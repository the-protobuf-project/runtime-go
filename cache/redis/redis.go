package redis

import (
	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Option configures a [Connect].
type Option func(*config)

type config struct {
	prefix string
	log    telemetry.Logger
	meter  telemetry.Meter
}

// WithPrefix namespaces every key this cache reads and writes.
//
// Use it to run several independent caches against one Redis database, or to
// share a database with another concern — a store or a stream. Entries written
// under one prefix are invisible to a cache configured with another.
func WithPrefix(prefix string) Option {
	return func(c *config) { c.prefix = prefix }
}

// WithLogger sets where this cache writes its own records — which key an id
// resolved to, which stale entries were swept. Defaults to
// [telemetry.NoopLogger].
//
// This is the provider's internal detail. For a uniform record per operation,
// wrap the result with [cache.WithLogging]; the two compose.
func WithLogger(log telemetry.Logger) Option {
	return func(c *config) { c.log = log }
}

// WithMeter sets where this cache reports its own measurements. Defaults to
// [telemetry.NoopMeter].
func WithMeter(m telemetry.Meter) Option {
	return func(c *config) { c.meter = m }
}

// Connect returns a [cache.Cache] backed by rdb.
//
// The client is yours: this package does not dial it, does not close it, and
// does not cache it anywhere. Two caches built from two different clients are
// genuinely independent, and the database index is whichever one you gave the
// client.
//
// A nil client yields a cache whose every call reports an error rather than
// panicking, so a wiring mistake surfaces as a failed operation with a clear
// message instead of a nil dereference somewhere deeper.
func Connect(rdb goredis.UniversalClient, opts ...Option) cache.Cache {
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

	return &cacheHandler{
		rdb:  rdb,
		keys: newKeys(cfg.prefix),
		log:  cfg.log,
	}
}
