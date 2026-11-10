package pubsub

import (
	"crypto/tls"
	"strings"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Option configures a [Connect].
type Option func(*config)

type config struct {
	publisherID     string
	prefix          string
	qos             byte
	clientID        string
	publishTimeout  time.Duration
	connections     int
	maxMessages     int
	metadataTimeout time.Duration
	dial            time.Duration
	tls             *tls.Config
	username        string
	password        []byte
	log             telemetry.Logger
	meter           telemetry.Meter
}

const (
	// DefaultPrefix is the first topic level Part 14 uses when none is
	// configured.
	DefaultPrefix = "opcua"

	// DefaultMaxDatasetMessages is how many DataSetMessages one
	// NetworkMessage of a batch carries at most unless
	// [WithMaxDatasetMessages] says otherwise.
	DefaultMaxDatasetMessages = 100

	// defaultQoS is at-least-once, so a publish returns once the broker has
	// the message.
	defaultQoS = 1

	defaultPublishTimeout  = 10 * time.Second
	defaultConnections     = 4
	defaultMetadataTimeout = 5 * time.Second
	defaultDial            = 10 * time.Second

	// reconnectDelay is how long a dropped publishing connection waits
	// before dialing again, as on the mqtt package.
	reconnectDelay = 2 * time.Second
)

func newConfig(opts ...Option) config {
	cfg := config{
		// Each config draws a fresh random PublisherId; set one with
		// WithPublisherID for an identity readers can find after a restart.
		publisherID:     "runtime-go-" + strings.ToLower(core.NewID()),
		prefix:          DefaultPrefix,
		qos:             defaultQoS,
		publishTimeout:  defaultPublishTimeout,
		connections:     defaultConnections,
		maxMessages:     DefaultMaxDatasetMessages,
		metadataTimeout: defaultMetadataTimeout,
		dial:            defaultDial,
		log:             telemetry.NoopLogger,
		meter:           telemetry.NoopMeter,
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
	if cfg.connections < 1 {
		cfg.connections = 1
	}
	if cfg.maxMessages < 1 {
		cfg.maxMessages = 1
	}
	if cfg.publishTimeout <= 0 {
		cfg.publishTimeout = defaultPublishTimeout
	}
	if cfg.metadataTimeout <= 0 {
		cfg.metadataTimeout = defaultMetadataTimeout
	}
	if cfg.dial <= 0 {
		cfg.dial = defaultDial
	}
	return cfg
}

// WithPublisherID sets the PublisherId every message is sent under, and the
// topic level that names it. The default is runtime-go-<ULID>, a new one per
// [Connect]; set a stable one so readers can find this publisher after a
// restart.
func WithPublisherID(id string) Option {
	return func(c *config) { c.publisherID = id }
}

// WithPrefix sets the first topic level. Part 14's, and the default, is
// [DefaultPrefix].
func WithPrefix(prefix string) Option {
	return func(c *config) { c.prefix = prefix }
}

// WithQoS sets the quality of service of every publish and subscription.
// Defaults to 1, at-least-once, so a publish returns once the broker has the
// message.
func WithQoS(qos byte) Option {
	return func(c *config) { c.qos = qos }
}

// WithClientID sets the client id of the first publishing connection; the
// others append -1, -2, .... Defaults to a generated one.
func WithClientID(id string) Option {
	return func(c *config) { c.clientID = id }
}

// WithPublishTimeout sets how long a publish waits for the broker's
// acknowledgement. Defaults to 10 seconds.
func WithPublishTimeout(d time.Duration) Option {
	return func(c *config) { c.publishTimeout = d }
}

// WithPublishConnections sets how many connections publishes are spread
// over. At least one; defaults to 4. A topic, and so a writer, always takes
// the same one, which keeps its messages in sequence order on the wire.
func WithPublishConnections(n int) Option {
	return func(c *config) { c.connections = n }
}

// WithMaxDatasetMessages sets the most DataSetMessages one NetworkMessage of
// a batch carries; a longer batch is sent as several. Bounded so a batch
// stays under the broker's and readers' packet size limits. Defaults to
// [DefaultMaxDatasetMessages].
func WithMaxDatasetMessages(n int) Option {
	return func(c *config) { c.maxMessages = n }
}

// WithMetadataTimeout sets how long [Provider.Metadata] waits for the
// retained message. Defaults to 5 seconds.
func WithMetadataTimeout(d time.Duration) Option {
	return func(c *config) { c.metadataTimeout = d }
}

// WithDialTimeout sets how long a connection may take to be accepted.
// Defaults to 10 seconds.
func WithDialTimeout(d time.Duration) Option {
	return func(c *config) { c.dial = d }
}

// WithTLS connects over TLS with this configuration. Set Certificates on it
// for mutual TLS. A bare host:port address is dialed over TLS once this is
// set, and an explicit mqtt:// or ws:// one is refused rather than connected
// in the clear.
func WithTLS(cfg *tls.Config) Option {
	return func(c *config) { c.tls = cfg }
}

// WithCredentials sends a username and password when connecting. They travel
// in the CONNECT packet as they are, so pair this with [WithTLS].
func WithCredentials(username, password string) Option {
	return func(c *config) { c.username, c.password = username, []byte(password) }
}

// WithLogger sets where the provider writes its own records. Defaults to
// [telemetry.NoopLogger].
func WithLogger(log telemetry.Logger) Option {
	return func(c *config) { c.log = log }
}

// WithMeter sets where the sequence-gap counter,
// streams_opcua_pubsub_sequence_gaps_total, is recorded. Defaults to
// [telemetry.NoopMeter], which records nothing.
func WithMeter(m telemetry.Meter) Option {
	return func(c *config) { c.meter = m }
}
