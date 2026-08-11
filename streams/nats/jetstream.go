package nats

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// JetStream keeps a stream's name, subjects and description of its own accord,
// but has nowhere in its config for the rest of [streams.Stream]. These carry
// the remainder in the stream's metadata, where they survive a restart with it.
const (
	metaName   = "runtime-go.name"
	metaUser   = "runtime-go.user_id"
	metaActive = "runtime-go.active"
)

// jsStreams is JetStream: the same subjects as core NATS, backed by a log.
type jsStreams struct {
	js  jetstream.JetStream
	log telemetry.Logger
}

var _ streams.Streams = (*jsStreams)(nil)

// Create declares a stream, generating an id when one is not supplied.
func (p *jsStreams) Create(ctx context.Context, in streams.Stream) (streams.Stream, error) {
	id := in.ID
	if id == "" {
		id = core.NewStreamID(in.Name)
	}
	id = safeName(id)

	p.log.Debug(ctx, "creating stream", telemetry.Fields{
		"id": id, "name": in.Name, "subjects": in.Subjects,
	})

	if _, err := p.js.CreateStream(ctx, p.configFor(id, in)); err != nil {
		p.log.Error(ctx, "could not create the stream", err, telemetry.Fields{"id": id})
		return streams.Stream{}, fmt.Errorf("nats: cannot create stream %s: %w", id, err)
	}

	p.log.Info(ctx, "stream created", telemetry.Fields{"id": id, "name": in.Name})
	return p.streamFrom(id, in), nil
}

// Get retrieves a stream by id.
func (p *jsStreams) Get(ctx context.Context, id string) (streams.Stream, error) {
	s, err := p.js.Stream(ctx, safeName(id))
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
		}
		return streams.Stream{}, fmt.Errorf("nats: cannot read stream %s: %w", id, err)
	}

	info, err := s.Info(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
		}
		return streams.Stream{}, fmt.Errorf("nats: cannot read stream %s: %w", id, err)
	}
	return toStream(&info.Config), nil
}

// Bind returns a publisher and subscriber for an existing stream.
//
// The stream is read here so an unknown id fails at Bind rather than at the
// first publish, and so the subject list is available to validate against
// without a round trip on every call.
func (p *jsStreams) Bind(ctx context.Context, id string) (streams.Manager, error) {
	name := safeName(id)

	handle, err := p.js.Stream(ctx, name)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
		}
		return nil, fmt.Errorf("nats: cannot bind stream %s: %w", id, err)
	}

	info, err := handle.Info(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
		}
		return nil, fmt.Errorf("nats: cannot bind stream %s: %w", id, err)
	}

	stream := toStream(&info.Config)
	p.log.Debug(ctx, "bound to stream", telemetry.Fields{"id": name, "subjects": stream.Subjects})
	return &jsManager{p: p, handle: handle, stream: stream}, nil
}

// Update replaces a stream's configuration, preserving its id.
func (p *jsStreams) Update(ctx context.Context, id string, in streams.Stream) (streams.Stream, error) {
	name := safeName(id)

	if _, err := p.js.UpdateStream(ctx, p.configFor(name, in)); err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
		}
		return streams.Stream{}, fmt.Errorf("nats: cannot update stream %s: %w", id, err)
	}

	p.log.Info(ctx, "stream updated", telemetry.Fields{"id": name})
	return p.streamFrom(name, in), nil
}

// Delete removes a stream.
func (p *jsStreams) Delete(ctx context.Context, id string) error {
	if err := p.js.DeleteStream(ctx, safeName(id)); err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
		}
		return fmt.Errorf("nats: cannot delete stream %s: %w", id, err)
	}

	p.log.Info(ctx, "stream deleted", telemetry.Fields{"id": id})
	return nil
}

// List returns every stream on the server.
func (p *jsStreams) List(ctx context.Context) ([]streams.Stream, error) {
	lister := p.js.ListStreams(ctx)

	var out []streams.Stream
	for info := range lister.Info() {
		out = append(out, toStream(&info.Config))
	}
	if err := lister.Err(); err != nil {
		p.log.Error(ctx, "could not list streams", err, nil)
		return nil, fmt.Errorf("nats: cannot list streams: %w", err)
	}

	slices.SortFunc(out, func(a, b streams.Stream) int { return strings.Compare(a.ID, b.ID) })
	p.log.Debug(ctx, "listed streams", telemetry.Fields{"count": len(out)})
	return out, nil
}

// configFor builds the JetStream config for a declaration.
func (p *jsStreams) configFor(name string, in streams.Stream) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        name,
		Description: in.Description,
		Subjects:    slices.Clone(in.Subjects),
		Metadata: map[string]string{
			metaName:   in.Name,
			metaUser:   in.UserID,
			metaActive: "true",
		},
	}
}

// streamFrom is the declaration as it now stands on the server.
func (p *jsStreams) streamFrom(id string, in streams.Stream) streams.Stream {
	return streams.Stream{
		ID:          id,
		Name:        in.Name,
		Description: in.Description,
		Subjects:    slices.Clone(in.Subjects),
		UserID:      in.UserID,
		Active:      true,
	}
}

// toStream maps a JetStream config back to the contract's shape.
func toStream(cfg *jetstream.StreamConfig) streams.Stream {
	return streams.Stream{
		ID:          cfg.Name,
		Name:        cfg.Metadata[metaName],
		Description: cfg.Description,
		Subjects:    slices.Clone(cfg.Subjects),
		UserID:      cfg.Metadata[metaUser],
		Active:      cfg.Metadata[metaActive] == "true",
	}
}

// safeName makes an id usable as a JetStream stream name.
//
// JetStream forbids whitespace and the characters that mean something in a
// subject, and a generated id carries the caller's stream name for readability
// — which may contain any of them.
func safeName(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '.', '*', '>', '/', '\\', ' ', '\t', '\n':
			return '-'
		}
		return r
	}, s)
}

// jsManager publishes to and consumes from one JetStream stream.
type jsManager struct {
	p      *jsStreams
	handle jetstream.Stream
	stream streams.Stream
}

var (
	_ streams.Manager    = (*jsManager)(nil)
	_ streams.Durable    = (*jsManager)(nil)
	_ streams.Positioned = (*jsManager)(nil)
)

// checkSubject rejects a subject the stream does not declare.
func (m *jsManager) checkSubject(ctx context.Context, subject string) error {
	if core.DeclaresPattern(m.stream.Subjects, subject) {
		return nil
	}
	m.p.log.Error(ctx, "subject is not declared by this stream", nil, telemetry.Fields{
		"subject": subject, "stream": m.stream.ID, "declared": m.stream.Subjects,
	})
	return core.ErrSubject(m.stream.ID, subject, m.stream.Subjects)
}

// Publish appends a value to the stream under a subject.
//
// The message id is sent as the JetStream message id, so a publish retried
// after an ambiguous failure is collapsed by the server rather than appended
// twice. That is the one thing [streams.WithPublisherRetry] cannot do on its
// own: it can retry safely only because the backend can recognize the repeat.
func (m *jsManager) Publish(ctx context.Context, subject string, value any, opts ...streams.Option) (string, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return "", err
	}
	if strings.ContainsAny(subject, "*>") {
		return "", fmt.Errorf("%w: cannot publish to the wildcard subject %q, only subscribe to it", streams.ErrUnknownSubject, subject)
	}

	o := streams.NewOptions(opts...)
	if o.TTL > 0 {
		return "", fmt.Errorf("%w: JetStream delivers when it is read, not on a timer", streams.ErrUnsupported)
	}

	id := o.ID
	if id == "" {
		id = core.NewID()
	}

	body, err := core.Pack(id, value)
	if err != nil {
		return "", err
	}

	if _, err := m.p.js.Publish(ctx, subject, body, jetstream.WithMsgID(id)); err != nil {
		m.p.log.Error(ctx, "could not publish", err, telemetry.Fields{"subject": subject, "id": id})
		return "", fmt.Errorf("nats: cannot publish on %q: %w", subject, err)
	}

	m.p.log.Debug(ctx, "published", telemetry.Fields{"subject": subject, "id": id, "bytes": len(body)})
	return id, nil
}

// Subscribe returns a channel of messages for a subject, starting at the end of
// the log.
//
// This is the undurable half: an ordered consumer that acknowledges nothing and
// is discarded when ctx is done. Nothing about what it saw survives it. Reach
// for [jsManager.Consume] when that matters.
func (m *jsManager) Subscribe(ctx context.Context, subject string) (<-chan streams.Message, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}

	consumer, err := m.handle.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject},
		DeliverPolicy:  jetstream.DeliverNewPolicy,
	})
	if err != nil {
		m.p.log.Error(ctx, "could not create an ordered consumer", err,
			telemetry.Fields{"subject": subject})
		return nil, fmt.Errorf("nats: cannot subscribe to %q: %w", subject, err)
	}

	it, err := consumer.Messages()
	if err != nil {
		return nil, fmt.Errorf("nats: cannot subscribe to %q: %w", subject, err)
	}

	m.p.log.Info(ctx, "subscribed", telemetry.Fields{"subject": subject})

	out := make(chan streams.Message)
	go func() {
		defer close(out)
		// Next blocks with no context of its own, so cancellation has to reach
		// it by closing the iterator underneath it.
		stop := context.AfterFunc(ctx, it.Stop)
		defer stop()

		delivered := 0
		defer func() {
			m.p.log.Info(ctx, "subscription closed",
				telemetry.Fields{"subject": subject, "delivered": delivered})
		}()

		for {
			raw, err := it.Next()
			if err != nil {
				// The iterator closing is how a canceled subscription ends.
				if !errors.Is(err, jetstream.ErrMsgIteratorClosed) && ctx.Err() == nil {
					m.p.log.Error(ctx, "could not read the stream", err,
						telemetry.Fields{"subject": subject})
				}
				return
			}

			msg, derr := core.Unpack(raw.Subject(), raw.Data())
			if derr != nil {
				m.p.log.Warn(ctx, "dropping a malformed message",
					telemetry.Fields{"subject": raw.Subject(), "error": derr.Error()})
				continue
			}
			delivered++
			select {
			case out <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// Consume delivers messages under a named consumer, starting at new ones.
func (m *jsManager) Consume(ctx context.Context, subject, consumer string, opts ...streams.Option) (<-chan streams.Delivery, error) {
	return m.ConsumeFrom(ctx, subject, consumer, streams.FromNew, opts...)
}

// ConsumeFrom delivers messages under a named consumer, starting at a chosen
// position.
//
// The name is a JetStream durable consumer: two processes consuming under one
// name share its position and split the work, and a process that dies and comes
// back resumes where the name left off rather than where the process did.
//
// The position applies when the consumer is created and not after. A consumer
// that already exists keeps the position it has — resetting it on every attach
// would replay the log every time a process restarted, which is the opposite of
// what a durable consumer is for.
func (m *jsManager) ConsumeFrom(ctx context.Context, subject, consumer string, at streams.Position, opts ...streams.Option) (<-chan streams.Delivery, error) {
	if err := m.checkSubject(ctx, subject); err != nil {
		return nil, err
	}
	if consumer == "" {
		return nil, fmt.Errorf("%w: a durable consumer needs a name, because the name is the position that survives a restart", streams.ErrUnsupported)
	}

	o := streams.NewOptions(opts...)
	if o.Group != "" {
		return nil, fmt.Errorf("%w: the consumer name %q is already the group on JetStream; drop the Group option", streams.ErrUnsupported, consumer)
	}

	name := safeName(consumer)
	policy := jetstream.DeliverNewPolicy
	if at == streams.FromEarliest {
		policy = jetstream.DeliverAllPolicy
	}

	// Fetch before create, rather than CreateOrUpdate: an update would try to
	// move an existing consumer's delivery policy, which JetStream refuses and
	// which would be the wrong thing to ask for anyway.
	c, err := m.handle.Consumer(ctx, name)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		c, err = m.handle.CreateConsumer(ctx, jetstream.ConsumerConfig{
			Name:          name,
			Durable:       name,
			FilterSubject: subject,
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: policy,
		})
	}
	if err != nil {
		m.p.log.Error(ctx, "could not attach the consumer", err,
			telemetry.Fields{"subject": subject, "consumer": name})
		return nil, fmt.Errorf("nats: cannot consume %q as %q: %w", subject, consumer, err)
	}

	it, err := c.Messages()
	if err != nil {
		return nil, fmt.Errorf("nats: cannot consume %q as %q: %w", subject, consumer, err)
	}

	m.p.log.Info(ctx, "consuming", telemetry.Fields{
		"subject": subject, "consumer": name, "from": policy,
	})

	out := make(chan streams.Delivery)
	go func() {
		defer close(out)
		stop := context.AfterFunc(ctx, it.Stop)
		defer stop()

		delivered := 0
		defer func() {
			m.p.log.Info(ctx, "consumer stopped", telemetry.Fields{
				"subject": subject, "consumer": name, "delivered": delivered,
			})
		}()

		for {
			raw, err := it.Next()
			if err != nil {
				if !errors.Is(err, jetstream.ErrMsgIteratorClosed) && ctx.Err() == nil {
					m.p.log.Error(ctx, "could not read as the consumer", err,
						telemetry.Fields{"subject": subject, "consumer": name})
				}
				return
			}

			msg, derr := core.Unpack(raw.Subject(), raw.Data())
			if derr != nil {
				// Nothing will ever decode this, so a handler will never
				// acknowledge it and it would redeliver forever. Terminate it:
				// unlike Ack, that records the message as undeliverable rather
				// than as handled.
				m.p.log.Warn(ctx, "terminating a malformed message",
					telemetry.Fields{"subject": raw.Subject(), "error": derr.Error()})
				_ = raw.Term()
				continue
			}

			attempt := 1
			if md, merr := raw.Metadata(); merr == nil && md.NumDelivered > 0 {
				attempt = int(md.NumDelivered)
			}

			delivered++
			select {
			case out <- streams.Delivery{
				Message: msg,
				Attempt: attempt,
				Ack:     func(context.Context) error { return raw.Ack() },
				Nak:     func(context.Context) error { return raw.Nak() },
			}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
