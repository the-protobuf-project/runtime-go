package rabbitmq

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Option configures a [Connect].
type Option func(*config)

type config struct {
	log      telemetry.Logger
	meter    telemetry.Meter
	prefix   string
	prefetch int
}

// defaultPrefetch is how many unacknowledged messages a consumer may hold.
//
// One would serialize a consumer that does slow work; unbounded would let a
// single consumer take the whole queue and starve its peers, which is the
// failure that makes people think round-robin is broken.
const defaultPrefetch = 32

// dialTimeout bounds opening the connection, so an unreachable broker fails
// Connect rather than hanging it.
const dialTimeout = 10 * time.Second

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
// consumer run ahead.
func WithPrefetch(n int) Option {
	return func(c *config) { c.prefetch = n }
}

// Connect returns a [streams.Streams] backed by RabbitMQ at url, which is an
// AMQP URL such as "amqp://guest:guest@localhost:5672/".
//
// Unlike most providers this one dials, for the reason given in the package
// documentation, and so must be closed. It returns an error rather than a
// provider when the broker is unreachable — worth hearing at startup rather
// than at the first publish.
func Connect(ctx context.Context, url string, opts ...Option) (streams.Streams, error) {
	if url == "" {
		return nil, fmt.Errorf("rabbitmq: no broker URL given")
	}

	cfg := config{log: telemetry.NoopLogger, meter: telemetry.NoopMeter, prefetch: defaultPrefetch}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.log == nil {
		cfg.log = telemetry.NoopLogger
	}
	if cfg.meter == nil {
		cfg.meter = telemetry.NoopMeter
	}

	// amqp.Dial has no context form, so the deadline is applied to the dialer
	// it uses rather than to the handshake as a whole.
	conn, err := amqp.DialConfig(url, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			d := net.Dialer{Timeout: dialTimeout}
			return d.DialContext(ctx, network, addr)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: cannot reach the broker: %w", err)
	}

	// One channel for publishing and for declaring. Consumers open their own,
	// because a channel is the unit of prefetch and of consumer cancellation.
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq: cannot open a channel: %w", err)
	}

	return &store{
		url: url, cfg: cfg, log: cfg.log,
		conn: conn, ch: ch,
		declared: map[string]streams.Stream{},
	}, nil
}

// store holds the connection, the publishing channel, and the declarations.
//
// The declarations are kept in this process. The exchange behind them is real
// and survives, but AMQP has nowhere to hang a description or a user id, so a
// stream's own metadata does not outlive the program that declared it.
type store struct {
	url  string
	cfg  config
	log  telemetry.Logger
	conn *amqp.Connection

	// mu guards ch as well as declared: an amqp.Channel is not safe for
	// concurrent use, and Publish and Create both reach for this one.
	mu       sync.RWMutex
	ch       *amqp.Channel
	declared map[string]streams.Stream
}

var (
	_ streams.Streams = (*store)(nil)
	_ streams.Closer  = (*store)(nil)
)

// Close releases the publishing channel and the connection.
//
// Consumers hold channels of their own and end with their contexts, so cancel
// those first or their deliveries stop mid-flight when this closes.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ch.Close(); err != nil && !isClosed(err) {
		_ = s.conn.Close()
		return fmt.Errorf("rabbitmq: cannot close the channel: %w", err)
	}
	if err := s.conn.Close(); err != nil && !isClosed(err) {
		return fmt.Errorf("rabbitmq: cannot close the connection: %w", err)
	}
	return nil
}

// exchange is the topic exchange carrying one stream's subjects.
func (s *store) exchange(streamID string) string {
	if s.cfg.prefix != "" {
		return s.cfg.prefix + "." + streamID
	}
	return streamID
}

// queue is the durable queue a named consumer of one subject reads from.
//
// The subject is part of the name because a queue is bound to a routing key:
// one queue per consumer per subject is what lets a consumer take one subject
// without receiving, and having to acknowledge, the rest.
func (s *store) queue(streamID, consumer, subject string) string {
	return s.exchange(streamID) + "." + consumer + "." + subject
}

// Create declares a stream, generating an id when one is not supplied.
//
// The exchange is durable, so it outlives a broker restart — a stream that
// vanished when the broker bounced would take its consumers' bindings with it.
func (s *store) Create(ctx context.Context, in streams.Stream) (streams.Stream, error) {
	id := in.ID
	if id == "" {
		id = safeName(core.NewStreamID(in.Name))
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
	err := s.ch.ExchangeDeclare(s.exchange(id), amqp.ExchangeTopic, true, false, false, false, nil)
	if err == nil {
		s.declared[id] = out
	}
	s.mu.Unlock()

	if err != nil {
		s.log.Error(ctx, "could not declare the exchange", err, telemetry.Fields{"id": id})
		return streams.Stream{}, fmt.Errorf("rabbitmq: cannot create stream %s: %w", id, err)
	}

	s.log.Info(ctx, "stream created", telemetry.Fields{"id": id, "name": in.Name, "subjects": out.Subjects})
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
func (s *store) Update(ctx context.Context, id string, in streams.Stream) (streams.Stream, error) {
	if _, err := s.Get(ctx, id); err != nil {
		return streams.Stream{}, err
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

	s.log.Info(ctx, "stream updated", telemetry.Fields{"id": id})
	return out, nil
}

// Delete removes a stream and the exchange behind it.
//
// Queues bound to it are left alone: a durable consumer's queue holds messages
// it has not handled, and discarding those because the stream was redeclared
// elsewhere would lose work the consumer never saw.
func (s *store) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	_, ok := s.declared[id]
	if ok {
		delete(s.declared, id)
	}
	var err error
	if ok {
		err = s.ch.ExchangeDelete(s.exchange(id), false, false)
	}
	s.mu.Unlock()

	if !ok {
		return fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
	}
	if err != nil {
		return fmt.Errorf("rabbitmq: cannot delete stream %s: %w", id, err)
	}

	s.log.Info(ctx, "stream deleted", telemetry.Fields{"id": id})
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

// safeName strips the characters AMQP reads as routing structure, so a
// generated id carrying the caller's stream name cannot change the topology.
func safeName(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '.', '*', '#', ' ':
			return '-'
		}
		return r
	}, s)
}

// isClosed reports whether err is the broker or the library saying this was
// already shut, which is not a failure when closing.
func isClosed(err error) bool {
	return err != nil && strings.Contains(err.Error(), "closed")
}
