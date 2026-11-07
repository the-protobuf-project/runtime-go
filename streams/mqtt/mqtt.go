package mqtt

import (
	"context"
	"net/url"
	"sync"

	"github.com/eclipse/paho.golang/autopaho"
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
	// One connection for publishing. Subscriptions and durable consumers get
	// their own, because a durable consumer's session is its identity.
	pub, err := s.open(ctx, cfg.clientID, false, publisherExpiry, "", nil)
	if err != nil {
		return nil, err
	}
	s.pub, s.owned = pub, true
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
	return s.disconnect(s.pub)
}

// topic is the MQTT topic a subject's messages travel on.
func (s *store) topic(streamID, subject string) string {
	name := streamID + "/" + subject
	if s.cfg.prefix != "" {
		name = s.cfg.prefix + "/" + name
	}
	return name
}
