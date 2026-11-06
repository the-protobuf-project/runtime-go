package opcua

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Create declares a stream, generating an id when one is not supplied.
//
// Every subject has to parse as a node id. It does not have to exist yet —
// whether it does is the server's answer, given at the first publish or
// subscribe — but a string that cannot name a node is a typo, and this is the
// call that made it.
func (s *store) Create(ctx context.Context, in streams.Stream) (streams.Stream, error) {
	id := in.ID
	if id == "" {
		id = core.NewStreamID(in.Name)
	}

	d, err := declare(id, in)
	if err != nil {
		return streams.Stream{}, err
	}

	s.mu.Lock()
	s.declared[id] = d
	s.mu.Unlock()

	s.log.Info(ctx, "stream declared", telemetry.Fields{
		"id": id, "name": in.Name, "subjects": d.stream.Subjects,
	})
	return d.stream, nil
}

// Get retrieves a stream by id.
func (s *store) Get(_ context.Context, id string) (streams.Stream, error) {
	d, err := s.lookup(id)
	if err != nil {
		return streams.Stream{}, err
	}
	return d.stream, nil
}

// Bind returns a publisher and subscriber for a declared stream.
func (s *store) Bind(_ context.Context, id string) (streams.Manager, error) {
	d, err := s.lookup(id)
	if err != nil {
		return nil, err
	}
	return &manager{store: s, declared: d}, nil
}

// Update replaces a stream's configuration, preserving its id.
//
// A manager already bound keeps the subjects it was bound with; bind again to
// see the new ones.
func (s *store) Update(_ context.Context, id string, in streams.Stream) (streams.Stream, error) {
	d, err := declare(id, in)
	if err != nil {
		return streams.Stream{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.declared[id]; !ok {
		return streams.Stream{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
	}
	s.declared[id] = d
	return d.stream, nil
}

// Delete removes a stream. The nodes it named are the server's and are left as
// they are.
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
	for _, d := range s.declared {
		out = append(out, d.stream)
	}
	slices.SortFunc(out, func(a, b streams.Stream) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

func (s *store) lookup(id string) (declared, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	d, ok := s.declared[id]
	if !ok {
		return declared{}, fmt.Errorf("%w: stream %s", streams.ErrNotFound, id)
	}
	return d, nil
}

// declare parses a stream's subjects as node ids.
func declare(id string, in streams.Stream) (declared, error) {
	nodes := make(map[string]*ua.NodeID, len(in.Subjects))
	for _, subject := range in.Subjects {
		n, err := ua.ParseNodeID(subject)
		if err != nil {
			return declared{}, fmt.Errorf("opcua: subject %q is not a node id (such as ns=2;s=Temperature): %w", subject, err)
		}
		nodes[subject] = n
	}

	return declared{
		stream: streams.Stream{
			ID:          id,
			Name:        in.Name,
			Description: in.Description,
			Subjects:    slices.Clone(in.Subjects),
			UserID:      in.UserID,
			Active:      true,
		},
		nodes: nodes,
	}, nil
}
