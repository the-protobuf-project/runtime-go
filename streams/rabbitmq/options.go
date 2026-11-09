package rabbitmq

import (
	"crypto/tls"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Option configures a [Connect] or [Use].
type Option func(*config)

type config struct {
	codec    streams.Codec
	log      telemetry.Logger
	meter    telemetry.Meter
	prefix   string
	prefetch int
	channels int
	retry    time.Duration
	tls      *tls.Config
	username string
	password string
}

const (
	// defaultPrefetch is how many unacknowledged messages a consumer may hold.
	//
	// One would serialize a consumer that does slow work; unbounded would let a
	// single consumer take the whole queue and starve its peers, which is the
	// failure that makes people think round-robin is broken.
	defaultPrefetch = 32

	// dialTimeout bounds opening a connection, so an unreachable broker fails
	// Connect rather than hanging it.
	dialTimeout = 10 * time.Second

	// defaultRetry is how long a dropped connection waits before dialing again.
	defaultRetry = 2 * time.Second

	// defaultPublishChannels is how many confirm-mode channels single
	// publishes are spread over when [WithPublishChannels] does not say.
	defaultPublishChannels = 4
)

// newConfig applies opts over the defaults.
func newConfig(opts ...Option) config {
	cfg := config{
		log: telemetry.NoopLogger, meter: telemetry.NoopMeter,
		prefetch: defaultPrefetch, retry: defaultRetry,
		channels: defaultPublishChannels,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.log == nil {
		cfg.log = telemetry.NoopLogger
	}
	if cfg.meter == nil {
		cfg.meter = telemetry.NoopMeter
	}
	if cfg.retry <= 0 {
		cfg.retry = defaultRetry
	}
	if cfg.channels < 1 {
		cfg.channels = 1
	}
	return cfg
}

// WithLogger sets where these streams write their own records. Defaults to
// [telemetry.NoopLogger].
func WithLogger(log telemetry.Logger) Option {
	return func(c *config) { c.log = log }
}

// WithMeter sets where these streams report their own measurements. Defaults to
// [telemetry.NoopMeter].
func WithMeter(m telemetry.Meter) Option {
	return func(c *config) { c.meter = m }
}

// WithPrefix namespaces every exchange and queue these streams declare, so
// several independent sets can share a virtual host.
func WithPrefix(prefix string) Option {
	return func(c *config) { c.prefix = prefix }
}

// WithPrefetch sets how many unacknowledged messages one consumer may hold.
// Defaults to 32.
//
// This is what makes two consumers under one name share work evenly: RabbitMQ
// hands a consumer up to this many messages before waiting for an
// acknowledgement, so a low number spreads a burst and a high one lets a fast
// consumer run ahead. AMQP caps it at 65535; a consumer attached with more
// fails. A [streams.Prefetch] on the call overrides it for that consumer.
func WithPrefetch(n int) Option {
	return func(c *config) { c.prefetch = n }
}

// WithPublishChannels sets how many channels, each in confirm mode, single
// publishes are spread over. Defaults to 4; zero or less is taken as one.
//
// A channel is one process on the broker, which handles its publishes and
// confirms in turn; several let concurrent publishers be confirmed side by
// side. A batch always goes through one channel, so its messages stay in
// order, and publishes from one caller that waits for each stay in order
// whatever channel each takes, since the next is not sent until the previous
// one is confirmed.
func WithPublishChannels(n int) Option {
	return func(c *config) { c.channels = n }
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

// WithTLS sets the TLS configuration for an amqps:// URL — a private CA to
// trust, or Certificates for mutual TLS. Without it an amqps:// URL is verified
// against the system's roots.
//
// It is refused alongside an amqp:// URL rather than ignored, which would
// connect in the clear. It applies only to [Connect]: a connection given to
// [Use] keeps the TLS it was dialed with.
func WithTLS(cfg *tls.Config) Option {
	return func(c *config) { c.tls = cfg }
}

// WithCredentials authenticates with a username and password, overriding any
// in the URL — so a secret can come from somewhere other than a connection
// string that ends up in logs. It applies only to [Connect]: a connection given
// to [Use] has already authenticated.
func WithCredentials(username, password string) Option {
	return func(c *config) { c.username, c.password = username, password }
}

// WithReconnectDelay sets how long a dropped connection waits before dialing
// again. Defaults to 2 seconds.
func WithReconnectDelay(d time.Duration) Option {
	return func(c *config) { c.retry = d }
}
