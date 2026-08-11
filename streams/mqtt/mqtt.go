package mqtt

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/paho"
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
	qos      byte
	expiry   uint32
	dial     time.Duration
	clientID string
}

const (
	// defaultQoS is at-least-once. Zero would make Durable a lie — an
	// unacknowledged QoS 0 message is simply gone — and two costs a second
	// round trip for a guarantee the contract does not promise.
	defaultQoS = 1

	// defaultExpiry is how long a broker keeps a departed session's
	// subscriptions and undelivered messages. A day is long enough to survive a
	// deploy and short enough that a consumer nobody runs again is not kept
	// forever.
	defaultExpiry = uint32(24 * 60 * 60)

	defaultDial = 10 * time.Second
)

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

// WithPrefix namespaces every topic these streams publish and subscribe to, so
// several independent sets can share a broker.
func WithPrefix(prefix string) Option {
	return func(c *config) { c.prefix = prefix }
}

// WithQoS sets the quality of service every publish and subscription uses.
// Defaults to 1, at-least-once.
//
// Zero is fire-and-forget: the broker does not acknowledge, nothing is
// redelivered, and a [streams.Durable] consumer built on it would be durable in
// name only. Two is exactly-once, at the cost of a second round trip.
func WithQoS(qos byte) Option {
	return func(c *config) { c.qos = qos }
}

// WithSessionExpiry sets how long the broker keeps a departed consumer's
// session — its subscriptions and the messages it has not acknowledged.
// Defaults to 24 hours.
//
// This is the window in which a restart still resumes rather than starts over.
// Past it the broker discards the session and the consumer comes back as a
// stranger, having missed whatever arrived while it was gone.
func WithSessionExpiry(d time.Duration) Option {
	return func(c *config) { c.expiry = uint32(d.Seconds()) }
}

// WithClientID sets the client id used for publishing and for undurable
// subscriptions. Defaults to a generated one.
//
// Durable consumers ignore it: their identity is the consumer name, which is
// the whole point of naming them.
func WithClientID(id string) Option {
	return func(c *config) { c.clientID = id }
}

// Connect returns a [streams.Streams] backed by an MQTT 5 broker at address.
//
// Unlike most providers this one dials, for the reason given in the package
// documentation, and so must be closed.
//
// A stream declared through it is a declaration this process is holding, not a
// broker-side object — MQTT has no registry to keep one in. Two processes each
// declare their own, and a restart forgets. What it buys is the subject check:
// publishing to a topic the stream never declared fails at the call that made
// the typo rather than landing somewhere nobody subscribes.
func Connect(address string, opts ...Option) (streams.Streams, error) {
	cfg := config{
		log: telemetry.NoopLogger, meter: telemetry.NoopMeter,
		qos: defaultQoS, expiry: defaultExpiry, dial: defaultDial,
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
	if cfg.clientID == "" {
		cfg.clientID = "runtime-go-" + core.NewID()
	}
	if address == "" {
		return nil, fmt.Errorf("mqtt: no broker address given")
	}

	s := &store{address: address, cfg: cfg, log: cfg.log, declared: map[string]streams.Stream{}}

	// One connection for publishing and for undurable subscriptions. Durable
	// consumers get their own, because their session is their identity.
	client, _, err := s.dial(context.Background(), cfg.clientID, false, nil)
	if err != nil {
		return nil, err
	}
	s.client = client
	return s, nil
}

// store holds the declarations and the publishing connection.
type store struct {
	address string
	cfg     config
	log     telemetry.Logger
	client  *paho.Client

	mu       sync.RWMutex
	declared map[string]streams.Stream
}

var (
	_ streams.Streams = (*store)(nil)
	_ streams.Closer  = (*store)(nil)
)

// dial opens a connection and completes the MQTT handshake.
//
// clean says whether to discard any session the broker still holds for this id.
// A durable consumer passes false, which is what makes the broker hand back
// what it kept while the consumer was away.
// It reports whether the broker had a session for this id already, which
// decides whether the caller still has to subscribe: a resumed session comes
// back with its subscriptions intact.
func (s *store) dial(ctx context.Context, clientID string, durable bool, onPublish func(paho.PublishReceived) (bool, error)) (*paho.Client, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.dial)
	defer cancel()

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", s.address)
	if err != nil {
		return nil, false, fmt.Errorf("mqtt: cannot reach %s: %w", s.address, err)
	}

	cfg := paho.ClientConfig{
		ClientID: clientID,
		Conn:     conn,
		// Acknowledgement belongs to the consumer, after the work. Without
		// this paho acknowledges on receipt, which would turn at-least-once
		// into at-most-once behind the caller's back.
		EnableManualAcknowledgment: true,
	}
	if onPublish != nil {
		cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){onPublish}
	}
	client := paho.NewClient(cfg)

	cp := &paho.Connect{
		ClientID:   clientID,
		KeepAlive:  30,
		CleanStart: !durable,
	}
	if durable {
		expiry := s.cfg.expiry
		cp.Properties = &paho.ConnectProperties{SessionExpiryInterval: &expiry}
	}

	ack, err := client.Connect(ctx, cp)
	if err != nil {
		_ = conn.Close()
		return nil, false, fmt.Errorf("mqtt: cannot connect to %s: %w", s.address, err)
	}
	if ack.ReasonCode != 0 {
		_ = conn.Close()
		return nil, false, fmt.Errorf("mqtt: broker refused the connection: %s", ack.Properties.ReasonString)
	}
	return client, ack.SessionPresent, nil
}

// Close disconnects the publishing connection.
//
// Durable consumers hold connections of their own and end with their contexts,
// so cancel those first or their sessions stay attached until the broker
// notices.
func (s *store) Close() error {
	if err := s.client.Disconnect(&paho.Disconnect{ReasonCode: 0}); err != nil {
		return fmt.Errorf("mqtt: cannot disconnect: %w", err)
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

// Create declares a stream, generating an id when one is not supplied.
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

// safeName strips the characters MQTT reserves for topic structure, so a
// generated id carrying the caller's stream name cannot change the topic tree.
func safeName(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '/', '+', '#':
			return '-'
		}
		return r
	}, s)
}
