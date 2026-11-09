package mqtt

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Create declares a stream, generating an id when one is not supplied.
//
// Its [streams.Stream.Headers] are validated here, so a bad default fails at
// the declaration rather than at the first publish.
func (s *store) Create(ctx context.Context, in streams.Stream) (streams.Stream, error) {
	if err := in.Headers.Validate(); err != nil {
		return streams.Stream{}, err
	}
	id := in.ID
	if id == "" {
		id = safeName(core.NewStreamID(in.Name))
	}

	out := declaration(id, in)

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

// Update replaces a stream's configuration, preserving its id, and refuses
// invalid headers as Create does.
func (s *store) Update(_ context.Context, id string, in streams.Stream) (streams.Stream, error) {
	if err := in.Headers.Validate(); err != nil {
		return streams.Stream{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.declared[id]; !ok {
		return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
	}
	out := declaration(id, in)
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

func declaration(id string, in streams.Stream) streams.Stream {
	return streams.Stream{
		ID:          id,
		Name:        in.Name,
		Description: in.Description,
		Subjects:    slices.Clone(in.Subjects),
		UserID:      in.UserID,
		Active:      true,
		Headers:     in.Headers.Clone(),
	}
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
