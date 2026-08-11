package redis

import (
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Option configures a [Connect] or [ConnectScheduled].
type Option func(*config)

type config struct {
	prefix  string
	db      int
	log     telemetry.Logger
	meter   telemetry.Meter
	maxLen  int64
	reclaim time.Duration
}

// defaultReclaim is how long a delivered-but-unacknowledged message sits with
// its consumer before another may take it over.
//
// It has to be longer than the slowest handler that is still making progress,
// or a working consumer has its message stolen and the work is done twice. Thirty
// seconds is generous for that and short enough that a process killed mid-message
// does not strand it for long. [WithReclaimAfter] moves it.
const defaultReclaim = 30 * time.Second

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

// WithMaxLen caps how many messages a durable subject retains, discarding the
// oldest beyond it. Zero, the default, retains everything.
//
// It applies only to [ConnectDurable]: pub/sub keeps nothing to trim, and a
// scheduled message is already removed when it fires. The cap is approximate —
// Redis trims on node boundaries, which is why it can do it without scanning —
// so a stream capped at 1000 holds at least that many and a little more.
//
// A retained log is the point of a durable stream, and an unbounded one is a
// memory leak with a slow fuse. This is where that choice is made.
func WithMaxLen(n int64) Option {
	return func(c *config) { c.maxLen = n }
}

// WithReclaimAfter sets how long a message may sit unacknowledged with one
// consumer before another may take it over. Defaults to [defaultReclaim].
//
// This is what makes redelivery real rather than promised: a consumer that dies
// holding a message acknowledges nothing, and without a reclaim its message
// stays in that dead consumer's pending list forever. Set it above the slowest
// handler that is still making progress.
func WithReclaimAfter(d time.Duration) Option {
	return func(c *config) { c.reclaim = d }
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

// ConnectDurable returns a [streams.Streams] backed by Redis Streams, which
// remembers what each consumer has handled and redelivers what it has not.
//
// This is the constructor to reach for when losing a message matters. [Connect]
// hands a message to whoever is listening at that moment and forgets it;
// this one appends to a log, tracks each named consumer's position server-side,
// and keeps a delivered message pending until it is acknowledged. The manager it
// binds satisfies [streams.Durable] and [streams.Positioned], so
// [streams.AsDurable] succeeds on it and fails on the other two.
//
// The client is yours: this package does not dial it, does not close it, and
// does not cache it anywhere.
func ConnectDurable(rdb goredis.UniversalClient, opts ...Option) streams.Streams {
	return connect(rdb, kindDurable, opts...)
}

func connect(rdb goredis.UniversalClient, k kind, opts ...Option) streams.Streams {
	cfg := config{log: telemetry.NoopLogger, meter: telemetry.NoopMeter, reclaim: defaultReclaim}
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
		rdb:     rdb,
		keys:    newKeys(cfg.prefix, k),
		kind:    k,
		db:      db,
		log:     cfg.log,
		maxLen:  cfg.maxLen,
		reclaim: cfg.reclaim,
	}
}
