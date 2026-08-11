package nats

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	gonats "github.com/nats-io/nats.go"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// plainStreams is core NATS: publish, subscribe, and nothing kept.
//
// The declarations live in this process because core NATS has nowhere to put
// them. That is a real limit and it is stated in [Connect] rather than papered
// over — the alternative would be inventing a registry subject and pretending
// this provider has storage it does not.
type plainStreams struct {
	nc       *gonats.Conn
	codec    streams.Codec
	registry *streams.Registry
	log      telemetry.Logger
	queue    string

	// owned says this package dialed the connection and must close it. One
	// handed in through Use belongs to the caller.
	owned bool

	mu       sync.RWMutex
	declared map[string]streams.Stream
}

var (
	_ streams.Streams = (*plainStreams)(nil)
	_ streams.Closer  = (*plainStreams)(nil)
)

// Close releases the connection, if this package made it.
//
// It is a no-op on a provider built by [Use], so a caller may close either kind
// without having to remember which it has.
func (p *plainStreams) Close() error {
	if p.owned {
		p.nc.Close()
	}
	return nil
}

// Create declares a stream, generating an id when one is not supplied.
func (p *plainStreams) Create(ctx context.Context, in streams.Stream) (streams.Stream, error) {
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

	p.mu.Lock()
	p.declared[id] = out
	p.mu.Unlock()

	p.log.Info(ctx, "stream declared", telemetry.Fields{
		"id": id, "name": in.Name, "subjects": out.Subjects,
	})
	return out, nil
}

// Get retrieves a stream by id.
func (p *plainStreams) Get(_ context.Context, id string) (streams.Stream, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	s, ok := p.declared[id]
	if !ok {
		return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
	}
	return s, nil
}

// Bind returns a publisher and subscriber for a declared stream.
func (p *plainStreams) Bind(ctx context.Context, id string) (streams.Manager, error) {
	s, err := p.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return &plainManager{p: p, stream: s}, nil
}

// Update replaces a stream's configuration, preserving its id.
func (p *plainStreams) Update(_ context.Context, id string, in streams.Stream) (streams.Stream, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.declared[id]; !ok {
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
	p.declared[id] = out
	return out, nil
}

// Delete removes a stream.
func (p *plainStreams) Delete(_ context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.declared[id]; !ok {
		return fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
	}
	delete(p.declared, id)
	return nil
}

// List returns every stream this process has declared.
func (p *plainStreams) List(_ context.Context) ([]streams.Stream, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	out := make([]streams.Stream, 0, len(p.declared))
	for _, s := range p.declared {
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b streams.Stream) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

// plainManager publishes to and subscribes from one stream over core NATS.
type plainManager struct {
	p      *plainStreams
	stream streams.Stream
}

var _ streams.Manager = (*plainManager)(nil)

// checkSubject rejects a subject the stream does not declare.
//
// A NATS subject is a pattern, so a stream declaring `orders.*` accepts
// `orders.placed` — matched with DeclaresPattern rather than by name.
func (m *plainManager) checkSubject(ctx context.Context, subject string) error {
	if core.DeclaresPattern(m.stream.Subjects, subject) {
		return nil
	}
	m.p.log.Error(ctx, "subject is not declared by this stream", nil, telemetry.Fields{
		"subject": subject, "stream": m.stream.ID, "declared": m.stream.Subjects,
	})
	return core.ErrSubject(m.stream.ID, subject, m.stream.Subjects)
}

// Publish sends a value on a subject.
func (m *plainManager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return "", err
	}
	if strings.ContainsAny(subject, "*>") {
		// A stream may declare a wildcard, but a message has to land somewhere
		// specific. NATS would reject this at the server; saying so here names
		// the call that did it.
		return "", fmt.Errorf("%w: cannot publish to the wildcard subject %q, only subscribe to it", streams.ErrUnknownSubject, subject)
	}

	o := streams.NewOptions(opts...)
	if o.TTL > 0 {
		return "", fmt.Errorf("%w: core NATS delivers immediately and cannot schedule", streams.ErrUnsupported)
	}

	id := o.ID
	if id == "" {
		id = core.NewID()
	}

	body, err := core.Pack(m.p.codec, id, value)
	if err != nil {
		return "", err
	}

	if err := m.p.nc.Publish(subject, body); err != nil {
		m.p.log.Error(ctx, "could not publish", err, telemetry.Fields{"subject": subject, "id": id})
		return "", fmt.Errorf("nats: cannot publish on %q: %w", subject, err)
	}

	m.p.log.Debug(ctx, "published", telemetry.Fields{"subject": subject, "id": id, "bytes": len(body)})
	return id, nil
}

// Subscribe returns a channel of messages for a subject.
//
// The channel is closed when ctx is done, which is also the only way to stop
// the subscription and release it on the server.
func (m *plainManager) Subscribe(ctx context.Context, subject string) (<-chan streams.Message, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}

	// Buffered so a slow reader does not push back into the client's own
	// dispatch loop, which would stall every other subscription on this
	// connection rather than just this one.
	raw := make(chan *gonats.Msg, 64)

	var (
		sub *gonats.Subscription
		err error
	)
	if m.p.queue != "" {
		sub, err = m.p.nc.ChanQueueSubscribe(subject, m.p.queue, raw)
	} else {
		sub, err = m.p.nc.ChanSubscribe(subject, raw)
	}
	if err != nil {
		m.p.log.Error(ctx, "could not subscribe", err, telemetry.Fields{"subject": subject})
		return nil, fmt.Errorf("nats: cannot subscribe to %q: %w", subject, err)
	}

	// The subscription is registered locally the moment Subscribe returns, but
	// the server does not know about it until the next flush. Without this, a
	// value published immediately afterwards races the interest reaching the
	// server and is silently dropped.
	if err := m.p.nc.Flush(); err != nil {
		_ = sub.Unsubscribe()
		return nil, fmt.Errorf("nats: cannot establish the subscription to %q: %w", subject, err)
	}

	m.p.log.Info(ctx, "subscribed", telemetry.Fields{
		"subject": subject, "queue": m.p.queue,
	})

	out := make(chan streams.Message)
	go func() {
		defer close(out)
		defer func() { _ = sub.Unsubscribe() }()

		delivered := 0
		defer func() {
			m.p.log.Info(ctx, "subscription closed",
				telemetry.Fields{"subject": subject, "delivered": delivered})
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case raw, ok := <-raw:
				if !ok {
					return
				}
				msg, derr := core.Unpack(m.p.registry, raw.Subject, raw.Data)
				if derr != nil {
					// One bad message is not a reason to tear down a healthy
					// subscription.
					m.p.log.Warn(ctx, "dropping a malformed message",
						telemetry.Fields{"subject": raw.Subject, "error": derr.Error()})
					continue
				}
				delivered++
				select {
				case out <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}
