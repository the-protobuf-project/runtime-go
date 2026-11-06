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
}

// defaultInterval is how often a server sends a subscription's changes. A
// tenth of a second is gopcua's own default, and quick enough that a change
// reads as immediate without asking the server to publish continuously.
const defaultInterval = 100 * time.Millisecond

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

// Use returns a [streams.Streams] over a client you built and connected —
// usually because it carries security configuration worth sharing with the rest
// of a program. It does not close what it did not open.
func Use(c *gopcua.Client, opts ...Option) streams.Streams {
	return newStore(c, newConfig(opts...))
}

func newConfig(opts ...Option) config {
	cfg := config{log: telemetry.NoopLogger, interval: defaultInterval}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.log == nil {
		cfg.log = telemetry.NoopLogger
	}
	if cfg.interval <= 0 {
		cfg.interval = defaultInterval
	}
	return cfg
}

func newStore(c *gopcua.Client, cfg config) *store {
	codec, registry := core.Resolve(cfg.codec)
	return &store{
		client: c, cfg: cfg, log: cfg.log,
		codec: codec, registry: registry,
		declared: map[string]declared{},
	}
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
}

// declared is a stream with its subjects already parsed, so a publish does not
// reparse a node id it validated at Create.
type declared struct {
	stream streams.Stream
	nodes  map[string]*ua.NodeID
}

var (
	_ streams.Streams = (*store)(nil)
	_ streams.Closer  = (*store)(nil)
)

// Close ends the session if [Connect] opened it, and is a no-op on a store
// built by [Use].
func (s *store) Close() error {
	if !s.owned {
		return nil
	}
	if err := s.client.Close(context.Background()); err != nil {
		return fmt.Errorf("opcua: cannot close the session: %w", err)
	}
	return nil
}
