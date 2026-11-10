package opcua

import (
	"context"
	"fmt"
	"sync"
	"time"

	gopcua "github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Option configures a [Connect] or [Use].
type Option func(*config)

type config struct {
	codec    streams.Codec
	log      telemetry.Logger
	interval time.Duration
	client   []gopcua.Option

	sampling         time.Duration
	queueSize        uint32
	buffer           int
	keepAlive        uint32
	lifetime         uint32
	initialValue     bool
	maxNotifications uint32
}

// defaultInterval is how often a server sends a subscription's changes. A
// tenth of a second is gopcua's own default, and quick enough that a change
// reads as immediate without asking the server to publish continuously.
const defaultInterval = 100 * time.Millisecond

// The defaults of the other subscription settings, each explained on its
// option.
const (
	defaultQueueSize        = 64
	defaultBuffer           = 4096
	defaultKeepAlive        = 10
	defaultLifetime         = 60
	defaultMaxNotifications = 1000
)

// WithLogger sets where these streams write their own records. Defaults to
// [telemetry.NoopLogger].
func WithLogger(log telemetry.Logger) Option {
	return func(c *config) { c.log = log }
}

// WithCodec sets how values OPC UA has no type for are encoded. Defaults to
// [streams.JSON]. Native values are written as themselves whatever this is.
func WithCodec(c streams.Codec) Option {
	return func(cfg *config) { cfg.codec = c }
}

// WithPublishingInterval sets how often the server sends a subscription's
// changes. Defaults to 100ms.
//
// Changes between two publishes are held in the monitored item's queue, so a
// longer interval means fewer, larger notifications, and more coalescing once
// that queue is full.
func WithPublishingInterval(d time.Duration) Option {
	return func(c *config) { c.interval = d }
}

// WithSamplingInterval sets how often the server samples a monitored node.
// Defaults to zero, which asks for the fastest it offers; a server that reports
// writes directly is not limited by it.
func WithSamplingInterval(d time.Duration) Option {
	return func(c *config) { c.sampling = d }
}

// WithQueueSize sets a monitored item's queue size on the server when a
// subscribe gives no [streams.Prefetch]: how many changes the server holds for
// this client between publishing cycles, discarding the oldest past it.
// Defaults to 64.
func WithQueueSize(n uint32) Option {
	return func(c *config) { c.queueSize = n }
}

// WithBuffer sets how many notifications a subscription's channel holds ahead
// of its reader before dropping the newest, so a stalled reader cannot stall
// the session every other subscription shares. Defaults to 4096, and is never
// less than the item's queue size.
func WithBuffer(n int) Option {
	return func(c *config) { c.buffer = n }
}

// WithKeepAliveCount sets how many publishing intervals pass without a
// notification before the server sends a keep-alive. Defaults to 10.
func WithKeepAliveCount(n uint32) Option {
	return func(c *config) { c.keepAlive = n }
}

// WithLifetimeCount sets how many publishing intervals pass without a client
// publish request before the server deletes the subscription. Defaults to 60;
// OPC UA requires at least three keep-alives.
func WithLifetimeCount(n uint32) Option {
	return func(c *config) { c.lifetime = n }
}

// WithInitialValue sets whether a new subscription delivers the node's current
// value first, as OPC UA reports it. Off by default: a subscription delivers
// what is written after it subscribes.
func WithInitialValue(on bool) Option {
	return func(c *config) { c.initialValue = on }
}

// WithMaxNotificationsPerPublish sets the most notifications the server puts
// in one Publish response; the rest wait for the next, which the client asks
// for at once. Defaults to 1000, which keeps a response well inside one 64 KiB
// chunk: some stacks fail to decode a response from the OPC Foundation's .NET
// server once it holds around 1,800. Zero is gopcua's own default, 10000.
func WithMaxNotificationsPerPublish(n uint32) Option {
	return func(c *config) { c.maxNotifications = n }
}

// WithClientOptions passes options through to the client [Connect] builds —
// security policy and mode, certificates, credentials. Given any, they are the
// whole configuration; given none, the client connects with no security and
// anonymously, which a production server should refuse.
func WithClientOptions(opts ...gopcua.Option) Option {
	return func(c *config) { c.client = append(c.client, opts...) }
}

// Connect returns a [streams.Streams] backed by the OPC UA server at endpoint,
// such as `opc.tcp://localhost:4840`.
//
// It dials and owns the session, so it must be closed. As with MQTT, a stream
// declared through it is a declaration this process holds rather than anything
// on the server: the server already has its nodes, and a stream names the ones
// this program means to use.
func Connect(ctx context.Context, endpoint string, opts ...Option) (streams.Streams, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("opcua: no endpoint given")
	}
	cfg := newConfig(opts...)

	// No security and no credentials only when the caller said nothing at all.
	// Mixed in with options that were given, a default would fight them — a
	// policy asked for under mode None is a configuration the server refuses.
	clientOpts := cfg.client
	if len(clientOpts) == 0 {
		clientOpts = []gopcua.Option{
			gopcua.SecurityMode(ua.MessageSecurityModeNone),
			gopcua.AuthAnonymous(),
		}
	}

	c, err := gopcua.NewClient(endpoint, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("opcua: cannot configure a client for %s: %w", endpoint, err)
	}
	if err := c.Connect(ctx); err != nil {
		return nil, fmt.Errorf("opcua: cannot connect to %s: %w", endpoint, err)
	}

	s := newStore(c, cfg)
	s.owned = true
	return s, nil
}

// MonitoredItems returns how many monitored items the streams of s, which must
// come from [Connect] or [Use], hold on the server.
func MonitoredItems(s streams.Streams) int {
	if st, ok := s.(*store); ok {
		return st.hub.items()
	}
	return 0
}

// Use returns a [streams.Streams] over a client you built and connected —
// usually because it carries security configuration worth sharing with the rest
// of a program. It does not close what it did not open.
func Use(c *gopcua.Client, opts ...Option) streams.Streams {
	return newStore(c, newConfig(opts...))
}

// newConfig applies opts over the defaults.
func newConfig(opts ...Option) config {
	cfg := config{
		log: telemetry.NoopLogger, interval: defaultInterval,
		queueSize: defaultQueueSize, buffer: defaultBuffer,
		keepAlive: defaultKeepAlive, lifetime: defaultLifetime,
		maxNotifications: defaultMaxNotifications,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.log == nil {
		cfg.log = telemetry.NoopLogger
	}
	if cfg.interval <= 0 {
		cfg.interval = defaultInterval
	}
	cfg.queueSize = max(cfg.queueSize, 1)
	cfg.buffer = max(cfg.buffer, 1)
	cfg.keepAlive = max(cfg.keepAlive, 1)
	return cfg
}

// newStore builds a store over c; [Connect] marks the ones it owns.
func newStore(c *gopcua.Client, cfg config) *store {
	codec, registry := core.Resolve(cfg.codec)
	s := &store{
		client: c, cfg: cfg, log: cfg.log,
		codec: codec, registry: registry,
		declared: map[string]declared{},
	}
	s.hub = newHub(s)
	return s
}

// store holds the declarations and the client every manager shares.
type store struct {
	client   *gopcua.Client
	owned    bool
	codec    streams.Codec
	registry *streams.Registry
	cfg      config
	log      telemetry.Logger

	mu       sync.RWMutex
	declared map[string]declared

	// hub is the one client subscription every stream's monitored items
	// share, created with the first and deleted with the last.
	hub *hub
}

// declared is a stream with its subjects already parsed, so a publish does not
// reparse a node id it validated at Create.
//
// A subject naming its namespace by index is in nodes; one naming it by URI
// (nsu=) is in named, and resolved against the server's namespace table on
// each use, because that table can change when the server restarts.
type declared struct {
	stream streams.Stream
	nodes  map[string]*ua.NodeID
	named  map[string]named
}

var (
	_ streams.Streams = (*store)(nil)
	_ streams.Closer  = (*store)(nil)
)

// Close ends every subscription opened through the store, closing their
// channels, deletes the client subscription they shared on the server, and
// then ends the session if [Connect] opened it. A session given to [Use] stays
// connected: it is the caller's.
func (s *store) Close() error {
	s.hub.close()
	if !s.owned {
		return nil
	}
	if err := s.client.Close(context.Background()); err != nil {
		return fmt.Errorf("opcua: cannot close the session: %w", err)
	}
	return nil
}
