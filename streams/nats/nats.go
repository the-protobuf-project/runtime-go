package nats

import (
	"fmt"

	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Option configures a [Connect] or [ConnectJetStream].
type Option func(*config)

type config struct {
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

// Connect returns a [streams.Streams] backed by core NATS, delivering to
// whoever is listening at the time.
//
// Nothing is stored: a subscriber that is not attached when a message is
// published never sees it, and a subscriber that dies holding one loses it.
// Reach for [ConnectJetStream] when either of those matters.
//
// A stream declared through this provider is a declaration this process is
// holding, not a server-side object — core NATS has no registry to keep it in.
// Two processes each declare their own, and a restart forgets. What it buys is
// the subject check: publishing to a subject the stream never declared fails at
// the call that made the typo.
//
// The connection is yours: this package does not dial it, does not close it,
// and does not drain it.
func Connect(nc *gonats.Conn, opts ...Option) streams.Streams {
	cfg := newConfig(opts...)
	return &plainStreams{
		nc:       nc,
		log:      cfg.log,
		queue:    cfg.queue,
		declared: make(map[string]streams.Stream),
	}
}

// ConnectJetStream returns a [streams.Streams] backed by JetStream, which keeps
// a log and remembers what each named consumer has handled.
//
// The manager it binds satisfies [streams.Durable] and [streams.Positioned], so
// [streams.AsDurable] succeeds on it and fails on [Connect]'s.
//
// It returns an error rather than a provider when JetStream is unreachable —
// asking a server without it enabled is a misconfiguration worth hearing about
// at startup instead of at the first publish.
//
// The connection is yours: this package does not dial it, does not close it,
// and does not drain it.
func ConnectJetStream(nc *gonats.Conn, opts ...Option) (streams.Streams, error) {
	cfg := newConfig(opts...)

	if cfg.queue != "" {
		return nil, fmt.Errorf("%w: a queue group has no meaning on JetStream, where a named consumer already shares a subject; use Durable.Consume", streams.ErrUnsupported)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("nats: cannot reach JetStream: %w", err)
	}
	return &jsStreams{js: js, log: cfg.log}, nil
}
