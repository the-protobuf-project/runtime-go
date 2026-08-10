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
//
// Refs are grouped by resource and read in bulk where the driver supports it, so
// a walk returning many neighbors costs a round trip per resource rather than
// one per record.
func Resolve[T proto.Message](ctx context.Context, d Driver, reg *Registry, refs []Ref) ([]T, error) {
	if d == nil {
		return nil, fmt.Errorf("database: Resolve needs a driver")
	}
	if reg == nil {
		return nil, fmt.Errorf("database: Resolve needs a registry to turn resource names into descriptors")
	}
	out := make([]T, len(refs))

	// Refs are grouped by resource so each group can go in one bulk read where
	// the driver has one. Without this a walk returning five hundred neighbors
	// costs five hundred round trips — which is the N+1 that makes a graph query
	// look fast and the page that renders it slow.
	byResource := make(map[string][]int, 4)
	for i, ref := range refs {
		byResource[ref.Resource] = append(byResource[ref.Resource], i)
	}

	batcher, bulk := d.(Batcher)
	for name, idx := range byResource {
		res, err := reg.Resource(name)
		if err != nil {
			return nil, fmt.Errorf("database: Resolve: %w", err)
		}

		if bulk {
			keys := make([]string, len(idx))
			for j, i := range idx {
				keys[j] = refs[i].Key
			}
			msgs, err := batcher.GetMany(ctx, res, keys)
			if err != nil {
				return nil, err
			}
			for j, i := range idx {
				if j >= len(msgs) || msgs[j] == nil {
					continue // gone since the walk; a nil entry says so
				}
				typed, ok := msgs[j].(T)
				if !ok {
					var zero T
					return nil, fmt.Errorf("database: Resolve: %s is %T, not %T", name, msgs[j], zero)
				}
				out[i] = typed
			}
			continue
		}

		// No bulk read on this backend, so one at a time — still grouped, which
		// at least resolves the descriptor once per resource rather than once
		// per ref.
		for _, i := range idx {
			msg, err := d.Get(ctx, res, refs[i].Key)
			if err != nil {
				if isNotFound(err) {
					continue
				}
				return nil, err
			}
			typed, ok := msg.(T)
			if !ok {
				var zero T
				return nil, fmt.Errorf("database: Resolve: %s is %T, not %T", name, msg, zero)
			}
			out[i] = typed
		}
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
