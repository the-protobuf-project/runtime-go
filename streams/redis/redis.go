package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Option configures a [Connect] or [ConnectScheduled].
type Option func(*config)

type config struct {
	codec    streams.Codec
	prefix   string
	db       int
	username string
	password string
	log      telemetry.Logger
	meter    telemetry.Meter
	maxLen   int64
	reclaim  time.Duration

	// confirmAcks says a durable Ack waits for Redis to answer its XACK.
	confirmAcks bool

	// autoPipeline sends single publishes through go-redis's auto-pipeliner.
	autoPipeline bool
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

// WithAuth sets the credentials [Connect] dials with. It has no effect on
// [Use], where the client you built already carries its own.
func WithAuth(username, password string) Option {
	return func(c *config) { c.username, c.password = username, password }
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

// WithConfirmAcks sets whether a durable [streams.Delivery.Ack] waits for
// Redis to answer the XACK before returning. On by default.
//
// Waiting is what lets an ack report a failure, but it costs a round trip per
// message to a consumer that acknowledges one message before reading the next,
// which caps it at one message per round trip. Off, an ack is queued and
// returns at once; queued acks go out together as one multi-id XACK, and a
// failure is logged rather than returned.
//
// Turning it off does not weaken at-least-once: an entry stays pending until
// its XACK lands, so one whose XACK is lost (a dropped connection, a process
// killed with acks still queued) is reclaimed and delivered again, exactly as
// if the handler had died before acknowledging. What is given up is hearing
// about it, and an ack that returned may still be followed by a redelivery,
// which handlers built for at-least-once already tolerate. NATS JetStream's
// plain ack makes the same trade.
func WithConfirmAcks(confirm bool) Option {
	return func(c *config) { c.confirmAcks = confirm }
}

// WithAutoPipeline sends each single publish — a PUBLISH, or a durable
// stream's XADD — through go-redis's auto-pipeliner, so concurrent publishes
// share pipelines instead of each taking a connection and a round trip of its
// own. Off by default, because go-redis marks the auto-pipeliner experimental.
//
// It is what runtime-rs does by default over its one multiplexed connection.
// Each publish still returns only once Redis has answered it, and is still
// never repeated once it may have been sent. Reads, subscriptions, batches
// and acknowledgements stay on the client as before: a blocking read inside a
// shared pipeline would hold up every publish queued behind it.
//
// A client that has no auto-pipeliner (a *redis.Ring) publishes as before,
// and the fallback is logged.
func WithAutoPipeline() Option {
	return func(c *config) { c.autoPipeline = true }
}

// WithCodec sets how payloads are encoded. Defaults to [streams.JSON].
//
// It changes what is published; what is *read* is decided by the message, which
// carries the name of the codec that wrote it. A provider always understands
// JSON as well as whatever is set here, so switching does not orphan a peer
// that has not switched yet.
func WithCodec(c streams.Codec) Option {
	return func(cfg *config) { cfg.codec = c }
}

// Use returns a [streams.Streams] backed by rdb, delivering immediately over
// pub/sub.
//
// The client is yours: this package does not dial it, does not close it, and
// does not cache it anywhere.
func Use(rdb goredis.UniversalClient, opts ...Option) streams.Streams {
	return connect(rdb, kindStream, false, opts...)
}

// UseScheduled returns a [streams.Streams] backed by rdb that delivers a
// message when its TTL expires rather than when it is published — a reminder, a
// lease timeout, a delayed retry.
//
// It is a separate constructor rather than an option because the two behave
// differently enough to be worth naming: a scheduled publish requires a TTL and
// an immediate one rejects it. Streams created through it live in their own key
// namespace, so they never appear in an immediate [Use]'s List.
//
// This relies on Redis keyspace notifications, so the server must run with
// `--notify-keyspace-events Ex`; without it, scheduled messages never fire.
func UseScheduled(rdb goredis.UniversalClient, opts ...Option) streams.Streams {
	return connect(rdb, kindNotify, false, opts...)
}

// UseDurable returns a [streams.Streams] backed by Redis Streams, which
// remembers what each consumer has handled and redelivers what it has not.
//
// This is the constructor to reach for when losing a message matters. [Use]
// hands a message to whoever is listening at that moment and forgets it;
// this one appends to a log, tracks each named consumer's position server-side,
// and keeps a delivered message pending until it is acknowledged. The manager it
// binds satisfies [streams.Durable] and [streams.Positioned], so
// [streams.AsDurable] succeeds on it and fails on the other two.
//
// The client is yours: this package does not dial it, does not close it, and
// does not cache it anywhere.
func UseDurable(rdb goredis.UniversalClient, opts ...Option) streams.Streams {
	return connect(rdb, kindDurable, false, opts...)
}

func connect(rdb goredis.UniversalClient, k kind, owned bool, opts ...Option) streams.Streams {
	cfg := config{log: telemetry.NoopLogger, meter: telemetry.NoopMeter, reclaim: defaultReclaim, confirmAcks: true}
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

	codec, registry, metrics := core.ResolveAll(cfg.codec, cfg.meter)

	pub, ap := rdb, (*goredis.AutoPipeliner)(nil)
	if cfg.autoPipeline {
		var err error
		if ap, err = rdb.AutoPipeline(); err != nil {
			cfg.log.Warn(context.Background(), "publishing without auto-pipelining: the client has none",
				telemetry.Fields{"error": err.Error()})
			ap = nil
		} else {
			pub = ap
		}
	}

	return &streamHandler{
		rdb:      rdb,
		pub:      pub,
		ap:       ap,
		codec:    codec,
		registry: registry,
		metrics:  metrics,
		keys:     newKeys(cfg.prefix, k),
		kind:     k,
		db:       db,
		log:      cfg.log,
		maxLen:   cfg.maxLen,
		reclaim:  cfg.reclaim,
		owned:    owned,

		confirmAcks: cfg.confirmAcks,
	}
}

// dial builds a client for address and hands it to connect, which then owns it.
func dial(ctx context.Context, address string, k kind, opts ...Option) (streams.Streams, error) {
	if address == "" {
		return nil, fmt.Errorf("redis: no address given")
	}

	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}

	rdb := goredis.NewClient(&goredis.Options{
		Addr:     address,
		DB:       cfg.db,
		Username: cfg.username,
		Password: cfg.password,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis: cannot reach %s: %w", address, err)
	}
	return connect(rdb, k, true, opts...), nil
}

// Connect dials address and returns immediate streams over pub/sub.
//
// The provider owns the connection it made, so it implements [streams.Closer]
// and closing it closes the connection. Use [Use] to supply a client of your
// own — a cluster client, one with TLS, or one shared with the rest of a
// program.
//
//	s, err := redis.Connect(ctx, "localhost:6379")
//	defer s.(streams.Closer).Close()
func Connect(ctx context.Context, address string, opts ...Option) (streams.Streams, error) {
	return dial(ctx, address, kindStream, opts...)
}

// ConnectScheduled dials address and returns streams that deliver on TTL
// expiry. See [UseScheduled] for what that means and what the server needs.
func ConnectScheduled(ctx context.Context, address string, opts ...Option) (streams.Streams, error) {
	return dial(ctx, address, kindNotify, opts...)
}

// ConnectDurable dials address and returns durable streams over Redis Streams.
// See [UseDurable].
func ConnectDurable(ctx context.Context, address string, opts ...Option) (streams.Streams, error) {
	return dial(ctx, address, kindDurable, opts...)
}

// refuseUnhonored refuses the publish options no Redis mode can honor, by
// name.
//
// An expiry needs something that holds the message until it is delivered and
// drops it when it is not: pub/sub holds nothing (a message reaches whoever is
// subscribed at once, or nobody), a scheduled message fires on its own timer,
// and Redis has no per-entry expiry inside a stream, only trimming by length or
// id, which would drop other messages with it. Retain needs a last value per
// subject, which none of the three keeps.
func refuseUnhonored(o streams.Options) error {
	if o.Expires > 0 {
		return fmt.Errorf("%w: Redis cannot expire one message (%v): pub/sub holds nothing to expire, and a Redis stream has no per-entry expiry, only trimming", streams.ErrUnsupported, o.Expires)
	}
	if o.Retain {
		return fmt.Errorf("%w: Redis keeps no last value per subject, so a message cannot be retained for later subscribers; read a durable stream from streams.FromEarliest instead", streams.ErrUnsupported)
	}
	return nil
}

// onceCmd is a command go-redis must not send a second time after a connection
// fails under it.
//
// The client repeats a command whose connection dropped, which is right for a
// read, an ack, or a metadata write, and wrong for a PUBLISH, an XADD of a
// message, or a scheduled message: a connection that drops after the server
// ran the command and before its reply arrived looks exactly like one that
// dropped before sending it, and a repeat would deliver the message twice.
// Those go out as onceCmd, and [sendOnce] repeats them only when the connection
// was refused outright, which proves nothing was sent; otherwise the caller
// decides, with [streams.WithPublisherRetry] where a duplicate is acceptable.
type onceCmd struct{ *goredis.Cmd }

// NoRetry tells go-redis not to repeat the command, nor a pipeline holding it.
func (onceCmd) NoRetry() bool { return true }

// newOnce builds a command that is never repeated after it may have been sent.
func newOnce(ctx context.Context, args ...any) onceCmd {
	return onceCmd{goredis.NewCmd(ctx, args...)}
}

// sendOnce runs args as a [onceCmd], repeating it once only when the first
// attempt never reached the server.
func sendOnce(ctx context.Context, rdb goredis.UniversalClient, args ...any) error {
	cmd := newOnce(ctx, args...)
	err := rdb.Process(ctx, cmd)
	if err != nil && refused(err) {
		cmd = newOnce(ctx, args...)
		err = rdb.Process(ctx, cmd)
	}
	return err
}

// refused reports whether err proves the command was never sent: the
// connection it needed could not be opened.
func refused(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}
