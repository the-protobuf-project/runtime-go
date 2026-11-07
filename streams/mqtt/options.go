package mqtt

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
	qos      byte
	expiry   uint32
	dial     time.Duration
	retry    time.Duration
	clientID string
	tls      *tls.Config
	username string
	password []byte
	server   string
}

const (
	// defaultQoS is at-least-once. Zero would make Durable a lie — an
	// unacknowledged QoS 0 message is simply gone — and two costs a second
	// round trip for a guarantee the contract does not promise.
	defaultQoS = 1

	// defaultExpiry is how long a broker keeps a departed session's
	// subscriptions and undelivered messages. A day is long enough to survive a
	// deploy and short enough that a consumer nobody runs again is not kept
	// forever.
	defaultExpiry = uint32(24 * 60 * 60)

	defaultDial = 10 * time.Second

	// defaultRetry is how long a dropped connection waits before redialing.
	// autopaho's own default is ten seconds, which is ten seconds of a
	// consumer receiving nothing after a broker restart.
	defaultRetry = 2 * time.Second
)

func newConfig(opts ...Option) config {
	cfg := config{
		log: telemetry.NoopLogger, meter: telemetry.NoopMeter,
		qos: defaultQoS, expiry: defaultExpiry, dial: defaultDial, retry: defaultRetry,
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

// WithPrefix namespaces every topic these streams publish and subscribe to, so
// several independent sets can share a broker.
func WithPrefix(prefix string) Option {
	return func(c *config) { c.prefix = prefix }
}

// WithQoS sets the quality of service every publish and subscription uses.
// Defaults to 1, at-least-once.
//
// Zero is fire-and-forget: the broker does not acknowledge, nothing is
// redelivered, and a [streams.Durable] consumer built on it would be durable in
// name only. Two is exactly-once, at the cost of a second round trip.
func WithQoS(qos byte) Option {
	return func(c *config) { c.qos = qos }
}

// WithSessionExpiry sets how long the broker keeps a departed consumer's
// session — its subscriptions and the messages it has not acknowledged.
// Defaults to 24 hours.
//
// This is the window in which a restart still resumes rather than starts over.
// Past it the broker discards the session and the consumer comes back as a
// stranger, having missed whatever arrived while it was gone.
func WithSessionExpiry(d time.Duration) Option {
	return func(c *config) { c.expiry = uint32(d.Seconds()) }
}

// WithClientID sets the client id used for publishing. Defaults to a generated
// one.
//
// Durable consumers ignore it: their identity is the consumer name, which is
// the whole point of naming them.
func WithClientID(id string) Option {
	return func(c *config) { c.clientID = id }
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

// WithTLS connects over TLS with this configuration. Set Certificates on it for
// mutual TLS, which is how AWS IoT Core authenticates a client.
//
// A bare host:port address is dialed over TLS once this is set. An explicit
// mqtt:// or ws:// address is refused alongside it rather than connected in the
// clear, which is what ignoring it would do.
func WithTLS(cfg *tls.Config) Option {
	return func(c *config) { c.tls = cfg }
}

// WithCredentials sends a username and password when connecting.
//
// They travel in the CONNECT packet as they are, so over a plain mqtt://
// connection they are readable by anything on the path. Pair this with
// [WithTLS] anywhere but a laptop.
func WithCredentials(username, password string) Option {
	return func(c *config) { c.username, c.password = username, []byte(password) }
}

// WithReconnectDelay sets how long a dropped connection waits before dialing
// again. Defaults to 2 seconds.
func WithReconnectDelay(d time.Duration) Option {
	return func(c *config) { c.retry = d }
}

// WithServer sets the broker that subscriptions and durable consumers dial,
// for streams built by [Use].
//
// It is needed there because those cannot share your connection: an MQTT
// session is a connection, and a durable consumer's session is its identity.
// [Connect] already has the address and ignores this.
func WithServer(address string) Option {
	return func(c *config) { c.server = address }
}
