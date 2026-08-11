package nats

import (
	"context"
	"fmt"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Option configures a [Connect] or [ConnectJetStream].
type Option func(*config)

type config struct {
	codec streams.Codec
	log   telemetry.Logger
	meter telemetry.Meter
	queue string
}

// WithLogger sets where these streams write their own records — which subject a
// publish went to, which consumer attached where. Defaults to
// [telemetry.NoopLogger].
func WithLogger(log telemetry.Logger) Option {
	return func(c *config) { c.log = log }
}

// WithMeter sets where these streams report their own measurements. Defaults to
// [telemetry.NoopMeter].
func WithMeter(m telemetry.Meter) Option {
	return func(c *config) { c.meter = m }
}

// WithQueueGroup makes every subscription from this provider join a NATS queue
// group, so several subscribers share a subject instead of each receiving every
// message.
//
// It is set here rather than per call because [streams.Subscriber] takes no
// options — and it is deliberately not inferred from [streams.Options.Group],
// which reaches the provider only through [streams.Durable.Consume], where core
// NATS has nothing to offer.
//
// It applies to [Connect] only. On JetStream, sharing a subject is what a named
// consumer already does, and a queue group layered on top would be a second
// answer to the same question.
func WithQueueGroup(name string) Option {
	return func(c *config) { c.queue = name }
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

func newConfig(opts ...Option) config {
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
	return cfg
}

// Use returns a [streams.Streams] backed by core NATS, delivering to
// whoever is listening at the time.
//
// Nothing is stored: a subscriber that is not attached when a message is
// published never sees it, and a subscriber that dies holding one loses it.
// Reach for [UseJetStream] or [ConnectJetStream] when either of those matters.
//
// A stream declared through this provider is a declaration this process is
// holding, not a server-side object — core NATS has no registry to keep it in.
// Two processes each declare their own, and a restart forgets. What it buys is
// the subject check: publishing to a subject the stream never declared fails at
// the call that made the typo.
//
// The connection is yours: this package does not dial it, does not close it,
// and does not drain it.
func Use(nc *gonats.Conn, opts ...Option) streams.Streams {
	cfg := newConfig(opts...)
	codec, registry, metrics := core.ResolveAll(cfg.codec, cfg.meter)
	return &plainStreams{
		nc:       nc,
		codec:    codec,
		registry: registry,
		metrics:  metrics,
		log:      cfg.log,
		queue:    cfg.queue,
		declared: make(map[string]streams.Stream),
	}
}

// UseJetStream returns a [streams.Streams] backed by JetStream, which keeps
// a log and remembers what each named consumer has handled.
//
// The manager it binds satisfies [streams.Durable] and [streams.Positioned], so
// [streams.AsDurable] succeeds on it and fails on [Use]'s.
//
// It returns an error rather than a provider when JetStream is unreachable —
// asking a server without it enabled is a misconfiguration worth hearing about
// at startup instead of at the first publish.
//
// The connection is yours: this package does not dial it, does not close it,
// and does not drain it.
func UseJetStream(nc *gonats.Conn, opts ...Option) (streams.Streams, error) {
	cfg := newConfig(opts...)

	if cfg.queue != "" {
		return nil, fmt.Errorf("%w: a queue group has no meaning on JetStream, where a named consumer already shares a subject; use Durable.Consume", streams.ErrUnsupported)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("nats: cannot reach JetStream: %w", err)
	}
	codec, registry, metrics := core.ResolveAll(cfg.codec, cfg.meter)
	return &jsStreams{js: js, log: cfg.log, codec: codec, registry: registry, metrics: metrics}, nil
}

// Connect dials url and returns a [streams.Streams] backed by core NATS.
//
// The provider owns the connection it made, so it implements [streams.Closer]
// and closing it closes the connection. Use [Use] to supply a connection of
// your own — one with credentials, TLS, or reconnect handlers.
//
//	s, err := nats.Connect(ctx, nats.DefaultURL)
//	defer s.(streams.Closer).Close()
func Connect(ctx context.Context, url string, opts ...Option) (streams.Streams, error) {
	nc, err := dial(ctx, url)
	if err != nil {
		return nil, err
	}
	s, ok := Use(nc, opts...).(*plainStreams)
	if !ok {
		nc.Close()
		return nil, fmt.Errorf("nats: unexpected provider type")
	}
	s.owned = true
	return s, nil
}

// ConnectJetStream dials url and returns a [streams.Streams] backed by
// JetStream. See [UseJetStream].
func ConnectJetStream(ctx context.Context, url string, opts ...Option) (streams.Streams, error) {
	nc, err := dial(ctx, url)
	if err != nil {
		return nil, err
	}
	provider, err := UseJetStream(nc, opts...)
	if err != nil {
		nc.Close()
		return nil, err
	}
	s, ok := provider.(*jsStreams)
	if !ok {
		nc.Close()
		return nil, fmt.Errorf("nats: unexpected provider type")
	}
	s.owned = nc
	return s, nil
}

// dial opens a connection, honoring ctx as a deadline for doing so.
func dial(ctx context.Context, url string) (*gonats.Conn, error) {
	if url == "" {
		return nil, fmt.Errorf("nats: no URL given")
	}

	deadline := gonats.DefaultTimeout
	if d, ok := ctx.Deadline(); ok {
		if remaining := time.Until(d); remaining > 0 {
			deadline = remaining
		}
	}

	nc, err := gonats.Connect(url, gonats.Timeout(deadline))
	if err != nil {
		return nil, fmt.Errorf("nats: cannot reach %s: %w", url, err)
	}
	return nc, nil
}
