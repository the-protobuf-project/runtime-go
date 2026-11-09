package rabbitmq

import (
	"context"
	"fmt"
	"slices"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Create declares a stream, generating an id when one is not supplied.
//
// The exchange is durable, so it outlives a broker restart — a stream that
// vanished when the broker bounced would take its consumers' bindings with it.
// Declaring is idempotent, so creating a stream another process already
// declared under the same id is how two processes share it.
//
// Default headers RabbitMQ would act on rather than carry are refused with
// [streams.ErrInvalidHeader] before anything is declared.
func (s *store) Create(ctx context.Context, in streams.Stream) (streams.Stream, error) {
	if err := checkStream(in); err != nil {
		return streams.Stream{}, err
	}
	id := in.ID
	if id == "" {
		id = safeName(core.NewStreamID(in.Name))
	}
	if err := short("exchange name", s.exchange(id)); err != nil {
		return streams.Stream{}, err
	}
	out := declaration(id, in)

	pub, err := s.publisher(ctx, 0)
	if err == nil {
		err = pub.ch.ExchangeDeclare(s.exchange(id), amqp.ExchangeTopic, true, false, false, false, nil)
	}
	if err != nil {
		s.log.Error(ctx, "could not declare the exchange", err, telemetry.Fields{"id": id})
		return streams.Stream{}, fmt.Errorf("rabbitmq: cannot create stream %s: %w", id, err)
	}

	s.declMu.Lock()
	s.declared[id] = out
	s.declMu.Unlock()

	s.log.Info(ctx, "stream created", telemetry.Fields{"id": id, "name": in.Name, "subjects": out.Subjects})
	return out, nil
}

// Get retrieves a stream by id.
func (s *store) Get(_ context.Context, id string) (streams.Stream, error) {
	s.declMu.RLock()
	defer s.declMu.RUnlock()

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

// Update replaces a stream's configuration, preserving its id, after checking
// its default headers. The exchange is unchanged; managers already bound keep
// the subjects and default headers they were bound with.
func (s *store) Update(ctx context.Context, id string, in streams.Stream) (streams.Stream, error) {
	if err := checkStream(in); err != nil {
		return streams.Stream{}, err
	}
	s.declMu.Lock()
	defer s.declMu.Unlock()

	if _, ok := s.declared[id]; !ok {
		return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
	}
	out := declaration(id, in)
	s.declared[id] = out

	s.log.Info(ctx, "stream updated", telemetry.Fields{"id": id})
	return out, nil
}

// Delete removes a stream and the exchange behind it.
//
// Queues bound to it are left alone: a durable consumer's queue holds messages
// it has not handled, and discarding those because the stream was redeclared
// elsewhere would lose work the consumer never saw.
func (s *store) Delete(ctx context.Context, id string) error {
	if _, err := s.Get(ctx, id); err != nil {
		return err
	}

	pub, err := s.publisher(ctx, 0)
	if err == nil {
		err = pub.ch.ExchangeDelete(s.exchange(id), false, false)
	}
	if err != nil {
		return fmt.Errorf("rabbitmq: cannot delete stream %s: %w", id, err)
	}

	s.declMu.Lock()
	delete(s.declared, id)
	s.declMu.Unlock()

	s.log.Info(ctx, "stream deleted", telemetry.Fields{"id": id})
	return nil
}

// List returns every stream this process has declared.
func (s *store) List(_ context.Context) ([]streams.Stream, error) {
	s.declMu.RLock()
	defer s.declMu.RUnlock()

	out := make([]streams.Stream, 0, len(s.declared))
	for _, stream := range s.declared {
		out = append(out, stream)
	}
	slices.SortFunc(out, func(a, b streams.Stream) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

// declaration is the stream recorded under id: in's own fields, active, with
// copies of its subjects and headers so the caller's cannot change it later.
func declaration(id string, in streams.Stream) streams.Stream {
	return streams.Stream{
		ID:          id,
		Name:        in.Name,
		Description: in.Description,
		Subjects:    slices.Clone(in.Subjects),
		UserID:      in.UserID,
		Headers:     in.Headers.Clone(),
		Active:      true,
	}
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
