package zeromq

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Option configures a [Publish] or [Subscribe].
type Option func(*config)

type config struct {
	log    telemetry.Logger
	meter  telemetry.Meter
	settle time.Duration
}

// defaultSettle is how long [Subscribe] waits for a subscription to reach the
// publisher before returning.
//
// It is a guess at a round trip, because ZeroMQ gives nothing better to wait
// on. Long enough for a local socket or a LAN; [WithSettle] raises it.
const defaultSettle = 250 * time.Millisecond

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

// WithSettle sets how long Subscribe waits before returning, for the
// subscription to reach the publisher.
//
// This is the slow-joiner window described in the package documentation. It is
// a delay rather than a handshake because ZeroMQ does not acknowledge a
// subscription; raising it narrows the window and never closes it.
func WithSettle(d time.Duration) Option {
	return func(c *config) { c.settle = d }
}

func newConfig(opts ...Option) config {
	cfg := config{log: telemetry.NoopLogger, meter: telemetry.NoopMeter, settle: defaultSettle}
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

// Publish returns a [streams.Streams] that binds endpoint and sends on it.
//
// One process in a ZeroMQ PUB/SUB topology binds and the rest connect. This is
// the one that binds, so it is the publisher; [Subscribe] is everyone else.
//
// The endpoint is a ZeroMQ transport string — "tcp://127.0.0.1:5563",
// "ipc:///tmp/feed", "inproc://feed".
func Publish(ctx context.Context, endpoint string, opts ...Option) (streams.Streams, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("zeromq: no endpoint given")
	}
	cfg := newConfig(opts...)

	sock := zmq4.NewPub(ctx)
	if err := sock.Listen(endpoint); err != nil {
		return nil, fmt.Errorf("zeromq: cannot bind %s: %w", endpoint, err)
	}

	cfg.log.Info(ctx, "publishing", telemetry.Fields{"endpoint": endpoint})
	return &store{endpoint: endpoint, cfg: cfg, log: cfg.log, pub: sock, declared: map[string]streams.Stream{}}, nil
}

// Subscribe returns a [streams.Streams] that connects to endpoint and receives
// from it.
//
// It publishes nothing: a SUB socket cannot send, so [streams.Publisher.Publish]
// on a manager from here refuses by name rather than dropping the message.
func Subscribe(ctx context.Context, endpoint string, opts ...Option) (streams.Streams, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("zeromq: no endpoint given")
	}
	cfg := newConfig(opts...)

	cfg.log.Info(ctx, "subscribing", telemetry.Fields{"endpoint": endpoint})
	return &store{endpoint: endpoint, cfg: cfg, log: cfg.log, declared: map[string]streams.Stream{}}, nil
}

// store holds the declarations and, for a publisher, the bound socket.
//
// The declarations live in this process because ZeroMQ has nowhere to put them.
// That is a real limit, stated in the package documentation rather than papered
// over: two processes each declare their own, and a restart forgets. What it
// buys is the subject check, which catches a typo at the call that made it.
type store struct {
	endpoint string
	cfg      config
	log      telemetry.Logger

	// pub is nil on a subscriber, which is what makes Publish refuse.
	pub zmq4.Socket

	mu       sync.RWMutex
	declared map[string]streams.Stream

	closeOnce sync.Once
	subs      []zmq4.Socket
}

var (
	_ streams.Streams = (*store)(nil)
	_ streams.Closer  = (*store)(nil)
)

// Close releases the publisher's socket and every subscriber socket opened
// through this store.
func (s *store) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.mu.Lock()
		subs := s.subs
		s.subs = nil
		s.mu.Unlock()

		for _, sock := range subs {
			if cerr := sock.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}
		if s.pub != nil {
			if cerr := s.pub.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}
	})
	if err != nil {
		return fmt.Errorf("zeromq: cannot close: %w", err)
	}
	return nil
}

// track records a socket so Close releases it.
func (s *store) track(sock zmq4.Socket) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs = append(s.subs, sock)
}

// Create declares a stream, generating an id when one is not supplied.
func (s *store) Create(ctx context.Context, in streams.Stream) (streams.Stream, error) {
	id := in.ID
	if id == "" {
		id = core.NewStreamID(in.Name)
	}

	out := streams.Stream{
		ID:          id,
		Name:        in.Name,
		Description: in.Description,
		Subjects:    slices.Clone(in.Subjects),
		UserID:      in.UserID,
		Active:      true,
	}

	s.mu.Lock()
	s.declared[id] = out
	s.mu.Unlock()

	s.log.Info(ctx, "stream declared", telemetry.Fields{
		"id": id, "name": in.Name, "subjects": out.Subjects,
	})
	return out, nil
}

// Get retrieves a stream by id.
func (s *store) Get(_ context.Context, id string) (streams.Stream, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stream, ok := s.declared[id]
	if !ok {
		return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
	}
	return stream, nil
}

// Bind returns a publisher and subscriber for a declared stream.
func (s *store) Bind(ctx context.Context, id string) (streams.Manager, error) {
	stream, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return &manager{store: s, stream: stream}, nil
}

// Update replaces a stream's configuration, preserving its id.
func (s *store) Update(_ context.Context, id string, in streams.Stream) (streams.Stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.declared[id]; !ok {
		return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
	}

	out := streams.Stream{
		ID:          id,
		Name:        in.Name,
		Description: in.Description,
		Subjects:    slices.Clone(in.Subjects),
		UserID:      in.UserID,
		Active:      true,
	}
	s.declared[id] = out
	return out, nil
}

// Delete removes a stream.
func (s *store) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.declared[id]; !ok {
		return fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
	}
	delete(s.declared, id)
	return nil
}

// List returns every stream this process has declared.
func (s *store) List(_ context.Context) ([]streams.Stream, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]streams.Stream, 0, len(s.declared))
	for _, stream := range s.declared {
		out = append(out, stream)
	}
	slices.SortFunc(out, func(a, b streams.Stream) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}
