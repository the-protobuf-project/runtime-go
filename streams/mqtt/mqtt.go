package mqtt

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"net/url"
	"sync"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/eclipse/paho.golang/paho/session/state"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Connect returns a [streams.Streams] backed by an MQTT 5 broker at address.
//
// The address is host:port, or a URL — mqtt://, tls://, mqtts://, ws:// or
// wss://. Unlike most providers this one dials, for the reason given in the
// package documentation, and so must be closed.
//
// The first connection is made before Connect returns, and a broker that is
// unreachable or refuses the credentials fails it. After that every connection
// keeps itself up: one that drops is dialed again until it is closed.
//
// A stream declared through it is a declaration this process is holding, not a
// broker-side object — MQTT has no registry to keep one in. Two processes each
// declare their own, and a restart forgets. What it buys is the subject check:
// publishing to a topic the stream never declared fails at the call that made
// the typo rather than landing somewhere nobody subscribes.
func Connect(ctx context.Context, address string, opts ...Option) (streams.Streams, error) {
	cfg := newConfig(opts...)
	server, err := parseServer(address, cfg.tls)
	if err != nil {
		return nil, err
	}
	if cfg.clientID == "" {
		cfg.clientID = "runtime-go-" + core.NewID()
	}

	s := newStore(cfg, server)
	// Connections for publishing, by default one. Subscriptions and durable
	// consumers get their own, because a durable consumer's session is its
	// identity.
	for n := range cfg.publishConns {
		id := cfg.clientID
		if n > 0 {
			id = fmt.Sprintf("%s-%d", cfg.clientID, n)
		}
		pipe := &pipeline{SessionManager: state.NewInMemory()}
		cm, err := s.open(ctx, id, false, publisherExpiry, "", nil, tuning{session: pipe})
		if err != nil {
			for _, open := range s.pool {
				_ = s.disconnect(open)
			}
			return nil, err
		}
		s.pool = append(s.pool, cm)
		s.pipes = append(s.pipes, pipe)
	}
	s.pub, s.owned = s.pool[0], true
	return s, nil
}

// Use returns a [streams.Streams] that publishes through a connection you built
// — usually one configured with TLS, credentials or a will message the rest of
// a program shares. It does not close what it did not open.
//
// Subscriptions and durable consumers still dial connections of their own, so
// they need [WithServer], and [WithTLS] and [WithCredentials] if the broker
// asks for them. Without a server they are refused by name.
func Use(cm *autopaho.ConnectionManager, opts ...Option) (streams.Streams, error) {
	cfg := newConfig(opts...)
	var server *url.URL
	if cfg.server != "" {
		var err error
		if server, err = parseServer(cfg.server, cfg.tls); err != nil {
			return nil, err
		}
	}
	s := newStore(cfg, server)
	s.pub = cm
	// A connection built elsewhere carries a session this package cannot
	// see into, so a batch on it is published one message at a time.
	s.pool, s.pipes = []*autopaho.ConnectionManager{cm}, []*pipeline{nil}
	return s, nil
}

func newStore(cfg config, server *url.URL) *store {
	codec, registry, metrics := core.ResolveAll(cfg.codec, cfg.meter)
	return &store{
		server: server, cfg: cfg, log: cfg.log,
		codec: codec, registry: registry, metrics: metrics,
		declared: map[string]streams.Stream{},
	}
}

// publisherExpiry is how long the broker keeps the publishing connection's
// session after a drop. It is short because the id is this process's alone and
// nothing resumes it after a restart; it only has to outlast a reconnect, so
// that a QoS 1 publish in flight when the connection dropped is sent again on
// the new one instead of waiting for an acknowledgement that cannot come.
const publisherExpiry = uint32(5 * 60)

// store holds the declarations and the publishing connection.
type store struct {
	server   *url.URL
	codec    streams.Codec
	registry *streams.Registry
	metrics  *core.Metrics
	cfg      config
	log      telemetry.Logger
	pub      *autopaho.ConnectionManager
	owned    bool

	// pool is every publishing connection, pub first; a topic always takes
	// the same one (see link). pipes is each one's session wrapper, nil for a
	// connection [Use] was handed.
	pool  []*autopaho.ConnectionManager
	pipes []*pipeline

	mu       sync.RWMutex
	declared map[string]streams.Stream
}

var (
	_ streams.Streams = (*store)(nil)
	_ streams.Closer  = (*store)(nil)
)

// Close disconnects the publishing connection if [Connect] opened it, and is a
// no-op on streams built by [Use].
//
// Subscriptions and durable consumers hold connections of their own and end
// with their contexts, so cancel those first or their sessions stay attached
// until the broker notices.
func (s *store) Close() error {
	if !s.owned {
		return nil
	}
	var errs []error
	for _, cm := range s.pool {
		if err := s.disconnect(cm); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// link is the publishing connection a topic's messages go out on: always the
// same one, because MQTT orders messages per connection and two publishes on a
// topic sent concurrently over different connections could reach the broker
// in either order. Different topics spread by hash.
func (s *store) link(topic string) (*autopaho.ConnectionManager, *pipeline) {
	n := slot(topic, len(s.pool))
	return s.pool[n], s.pipes[n]
}

// slot is which of count connections a topic takes: 32-bit FNV-1a of the
// topic, so the choice is the same for every publish and every process.
func slot(topic string, count int) int {
	if count <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(topic))
	return int(h.Sum32() % uint32(count))
}

// ClearRetained clears the value retained on subject of stream, so a
// subscriber that joins later is handed nothing there. s must come from
// [Connect] or [Use].
//
// A message published with [streams.Retain] stays the topic's retained value
// until another retained publish replaces it or this clears it. Clearing is
// MQTT's own way: a retained publish with an empty payload, which the broker
// takes as "retain nothing" and forwards to current subscribers as an empty
// message. This package's subscriptions skip that empty message rather than
// failing to decode it, since it carries no frame.
func ClearRetained(ctx context.Context, s streams.Streams, stream, subject string) error {
	clearer, ok := s.(interface {
		ClearRetained(ctx context.Context, stream, subject string) error
	})
	if !ok {
		return fmt.Errorf("%w: %T is not MQTT streams", streams.ErrUnsupported, s)
	}
	return clearer.ClearRetained(ctx, stream, subject)
}

// ClearRetained clears the value retained on subject of stream. See the
// package-level [ClearRetained].
func (s *store) ClearRetained(ctx context.Context, stream, subject string) error {
	declared, err := s.Get(ctx, stream)
	if err != nil {
		return err
	}
	m := &manager{store: s, stream: declared}
	topic, _, err := m.prepare(ctx, subject, streams.Options{})
	if err != nil {
		return err
	}
	if err := m.send(ctx, &paho.Publish{Topic: topic, QoS: s.cfg.qos, Retain: true}); err != nil {
		return fmt.Errorf("mqtt: cannot clear the retained value on %q: %w", subject, err)
	}
	return nil
}

// topic is the MQTT topic a subject's messages travel on.
func (s *store) topic(streamID, subject string) string {
	name := streamID + "/" + subject
	if s.cfg.prefix != "" {
		name = s.cfg.prefix + "/" + name
	}
	return name
}
