package redis

import (
	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Option configures a [Connect].
type Option func(*config)

type config struct {
	prefix string
	log    telemetry.Logger
	meter  telemetry.Meter
}

// WithPrefix namespaces every key this store reads and writes.
//
// Use it to run several independent stores against one Redis database, or to
// share a database with another concern — a cache or a stream.
func WithPrefix(prefix string) Option {
	return func(c *config) { c.prefix = prefix }
}

// WithLogger sets where this store writes its own records — which hash a value
// canonicalized to, which reservation was released. Defaults to
// [telemetry.NoopLogger].
//
// For a uniform record per operation, wrap the result with
// [database.WithLogging]; the two compose.
func WithLogger(log telemetry.Logger) Option {
	return func(c *config) { c.log = log }
}

// WithMeter sets where this store reports its own measurements. Defaults to
// [telemetry.NoopMeter].
func WithMeter(m telemetry.Meter) Option {
	return func(c *config) { c.meter = m }
}

// Connect returns a [database.Store] backed by rdb.
//
// The client is yours: this package does not dial it, does not close it, and
// does not cache it anywhere. The database index is whichever one you gave the
// client.
func Connect(rdb goredis.UniversalClient, opts ...Option) database.Store {
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

	return &storeHandler{
		rdb:  rdb,
		keys: newKeys(cfg.prefix),
		log:  cfg.log,
	}
}
