package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Connect returns a [streams.Streams] backed by RabbitMQ at url, an AMQP URL
// such as "amqp://guest:guest@localhost:5672/" or "amqps://broker:5671/".
//
// It dials, for the reason given in the package documentation, and so must be
// closed. It returns an error rather than a provider when the broker is
// unreachable — worth hearing at startup rather than at the first publish.
// After that the connection keeps itself up: one that drops is dialed again
// until Close.
func Connect(ctx context.Context, url string, opts ...Option) (streams.Streams, error) {
	if url == "" {
		return nil, fmt.Errorf("rabbitmq: no broker URL given")
	}
	cfg := newConfig(opts...)

	uri, err := amqp.ParseURI(url)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: %w", err)
	}
	if cfg.tls != nil && uri.Scheme != "amqps" {
		return nil, fmt.Errorf("rabbitmq: a TLS configuration was given for an %s:// URL, which would connect in the clear; use amqps://", uri.Scheme)
	}

	s := newStore(cfg, true)
	s.url = url
	conn, closed, err := s.dial(ctx)
	if err != nil {
		return nil, err
	}
	s.conn = conn
	// The supervisor outlives this call by design: it runs until Close, not
	// until ctx ends.
	go s.supervise(closed) //nolint:gosec // see above
	return s, nil
}

// Use returns a [streams.Streams] over a connection you dialed — usually one
// configured for TLS or authentication the rest of a program shares. Consumers
// open channels of their own on it, which AMQP is built for.
//
// [WithTLS] and [WithCredentials] apply only to [Connect]; they do not change a
// connection given here, which was configured when it was dialed.
//
// It does not close the connection, and it does not redial one that drops:
// recovering a connection is the job of whoever owns it. Publishes and
// consumers fail once it has closed.
func Use(conn *amqp.Connection, opts ...Option) streams.Streams {
	s := newStore(newConfig(opts...), false)
	s.conn = conn
	return s
}

// newStore builds a store around cfg; owned says whether it dialed, and so
// may redial and close, its connection.
func newStore(cfg config, owned bool) *store {
	codec, registry, metrics := core.ResolveAll(cfg.codec, cfg.meter)
	up := make(chan struct{})
	close(up)
	return &store{
		cfg: cfg, log: cfg.log, owned: owned, name: "runtime-go-" + core.NewID(),
		codec: codec, registry: registry, metrics: metrics,
		up: up, done: make(chan struct{}),
		pubs:     make([]*pubChannel, cfg.channels),
		declared: map[string]streams.Stream{},
	}
}

// store holds the connection, the publishing channels, and the declarations.
//
// The declarations are kept in this process. The exchange behind them is real
// and survives, but AMQP has nowhere to hang a description or a user id, so a
// stream's own metadata does not outlive the program that declared it.
type store struct {
	url      string
	name     string
	owned    bool
	codec    streams.Codec
	registry *streams.Registry
	metrics  *core.Metrics
	cfg      config
	log      telemetry.Logger

	// mu guards the connection and what hangs off it. up is closed while the
	// connection is usable and replaced while it is being dialed again.
	mu   sync.RWMutex
	conn *amqp.Connection
	// pubs are the publishing channels, one per slot, opened on first use;
	// next picks the slot the next publish takes.
	pubs    []*pubChannel
	next    atomic.Uint64
	up      chan struct{}
	closing bool
	done    chan struct{}

	declMu   sync.RWMutex
	declared map[string]streams.Stream
}

var (
	_ streams.Streams = (*store)(nil)
	_ streams.Closer  = (*store)(nil)
)

// Close stops reconnecting and closes the connection [Connect] opened. On a
// store built by [Use] it closes only the publishing channels it opened: the
// connection is the caller's and stays open.
//
// Consumers hold channels of their own and end with their contexts, so cancel
// those first or their deliveries stop mid-flight when this closes.
func (s *store) Close() error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	close(s.done)
	conn := s.conn
	s.mu.Unlock()

	if !s.owned {
		return s.closePublishers()
	}
	if err := conn.Close(); err != nil && !conn.IsClosed() {
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
