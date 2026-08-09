package database

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// Resolve loads the records a set of [Ref]s points at.
//
// A graph walk returns refs rather than records, and this is the opt-in second
// step. It is a helper rather than something [Graph.Neighbors] does for you
// because the two questions have very different costs: asking what is connected
// to this is one query, and loading everything it is connected to is one more
// per record. A caller that only needed the keys should not pay for the values.
//
//	edges, _ := db.Graph.Neighbors(ctx, alice, TraverseOptions{})
//	refs := Ends(edges)
//	users, _ := Resolve[*pb.User](ctx, db, reg, refs)
//
// Refs naming a resource the registry does not know are an error, not a gap: a
// walk that returned one means the registry and the graph disagree about what
// exists, and skipping it silently would turn that into a missing record nobody
// can account for.
//
// A ref whose record is genuinely gone becomes a nil entry, in the order asked,
// because a walk racing a delete is ordinary.
func Resolve[T proto.Message](ctx context.Context, d Driver, reg *Registry, refs []Ref) ([]T, error) {
	if d == nil {
		return nil, fmt.Errorf("database: Resolve needs a driver")
	}
	if reg == nil {
		return nil, fmt.Errorf("database: Resolve needs a registry to turn resource names into descriptors")
	}
	out := make([]T, len(refs))
	for i, ref := range refs {
		res, err := reg.Resource(ref.Resource)
		if err != nil {
			return nil, fmt.Errorf("database: Resolve: %w", err)
		}
		msg, err := d.Get(ctx, res, ref.Key)
		if err != nil {
			if isNotFound(err) {
				continue // gone since the walk; a nil entry says so
			}
			return nil, err
		}
		typed, ok := msg.(T)
		if !ok {
			var zero T
			return nil, fmt.Errorf("database: Resolve: %s is %T, not %T", ref.Resource, msg, zero)
		}
		out[i] = typed
	}
	return out, nil
}

// Ends returns the far end of each edge, which is what a caller almost always
// wants from a walk: who is connected, rather than which connection said so.
func Ends(edges []Edge) []Ref {
	out := make([]Ref, len(edges))
	for i, e := range edges {
		out[i] = e.To
	}
	return out
}

// Starts returns the near end of each edge, for a walk run inbound.
func Starts(edges []Edge) []Ref {
	out := make([]Ref, len(edges))
	for i, e := range edges {
		out[i] = e.From
	}
	return out
}
