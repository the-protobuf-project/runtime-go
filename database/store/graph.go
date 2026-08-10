package store

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// Graph is implemented by a driver whose backend stores relationships between
// records and can walk them.
//
// It is a capability rather than a separate contract because a graph database is
// not an alternative to a record store — it is a record store that also knows
// what is connected to what. ArangoDB is the proof: the same server holds
// documents addressed by key and edges between them, so a driver that had to
// choose one contract would make the other unreachable.
//
// # What is portable and what is not
//
// Every backend here agrees on: a directed connection, between two records,
// carrying a type and optionally its own fields, walkable from a starting
// record. That is the whole of this interface, and it is deliberately less than
// any one backend can do.
//
// What is left out is anything only one of them has. ArangoDB names graphs and
// declares which collections an edge may join; Neo4j has neither, and labels
// nodes in a way ArangoDB does not. Putting either in here would give the other
// a method it could only refuse — so the shared part is the contract, and a
// caller that needs AQL or Cypher reaches past it through the driver's own
// package.
//
// # Edges are resources
//
// An edge type is described by a [Resource], exactly as a record is. On
// ArangoDB that resource is an edge collection; on Neo4j it is a relationship
// type. Either way the descriptor already says what fields it carries, which
// means an edge with properties needs no second schema language and no
// per-backend mapping — the generator that produced the record descriptors
// produces this one too.
type Graph interface {
	// Connect creates an edge of the type edge describes, from one record to
	// another, and returns it with the key the backend assigned.
	//
	// props carries the edge's own fields and may be nil for a bare connection.
	// A backend reports [ErrNotFound] when either endpoint is missing, rather
	// than creating an edge that dangles.
	Connect(ctx context.Context, edge *Resource, from, to Ref, props proto.Message) (Edge, error)

	// Disconnect removes one edge by its key, reporting [ErrNotFound] when it is
	// not there.
	Disconnect(ctx context.Context, edge *Resource, key string) error

	// Neighbors returns the records one hop from a starting record.
	//
	// This is the operation a caller actually reaches for most — who does this
	// user follow, what is in this order — and it is one hop because a hop is
	// what a caller can reason about. Use [Graph.Traverse] for a walk whose
	// depth is the point.
	Neighbors(ctx context.Context, from Ref, opts TraverseOptions) ([]Edge, error)

	// Traverse walks outward from a starting record and returns the paths it
	// found, bounded by opts.
	//
	// A depth bound is required rather than defaulted, because an unbounded walk
	// on a connected graph visits everything: the difference between a query and
	// an outage is a number the caller has to have thought about.
	Traverse(ctx context.Context, from Ref, opts TraverseOptions) ([]Path, error)
}

// GraphMigrator is implemented by a backend that has to be told which
// collections an edge may join before it will store one.
//
// ArangoDB does; Neo4j does not, and says so by not implementing this rather
// than by accepting a declaration it would ignore. A program that calls this
// where it is absent gets [ErrUnimplemented] naming the backend, which is the
// honest answer to "was my graph schema applied".
type GraphMigrator interface {
	// EnsureGraph declares a named graph over the given edge definitions, and is
	// safe to call repeatedly.
	EnsureGraph(ctx context.Context, name string, defs []EdgeDefinition) error

	// DropGraph removes a graph declaration. Whether the edges themselves go
	// with it is the backend's own rule, and one worth reading before calling
	// this against data you want.
	DropGraph(ctx context.Context, name string) error
}

// Ref identifies one record: which resource holds it, and under which key.
//
// Two fields rather than a single opaque string because the backends disagree
// about the string. ArangoDB writes "collection/key" and Neo4j has no such
// notion at all, so a caller that had to build one would be writing
// backend-specific code at the one place this contract exists to prevent it.
type Ref struct {
	// Resource is the [Resource.Name] of the record's descriptor.
	Resource string

	// Key is the record's primary key.
	Key string
}

// Edge is a directed connection between two records.
type Edge struct {
	// Type is the [Resource.Name] of the edge's own descriptor.
	Type string

	// Key identifies this edge, assigned by the backend on [Graph.Connect].
	Key string

	// From and To are the records it joins.
	From Ref
	To   Ref

	// Props are the edge's own fields, and is nil for a bare connection or where
	// a walk did not ask for them.
	Props proto.Message
}

// EdgeDefinition declares which record types an edge may join.
//
// It mirrors what ArangoDB requires to create a graph: an edge collection, and
// the vertex collections it is allowed to run between. A backend that does not
// enforce this ignores it — see [GraphMigrator].
type EdgeDefinition struct {
	// Edge is the [Resource.Name] of the edge's descriptor.
	Edge string

	// From and To are the resource names this edge may join. Several of each,
	// because one relationship commonly runs between more than one kind of
	// thing.
	From []string
	To   []string
}

// Direction is which way a walk follows an edge.
type Direction int

const (
	// Outbound follows edges away from the starting record, which is the
	// direction they were created in and the default because it is the one a
	// caller means when it does not say.
	Outbound Direction = iota

	// Inbound follows edges toward the starting record.
	Inbound

	// AnyDirection follows edges either way, at the cost of an index lookup on
	// both ends of every edge.
	AnyDirection
)

// TraverseOptions bounds a walk.
type TraverseOptions struct {
	// Types restricts the walk to edges of these resource names. Empty follows
	// every type, which is rarely what a caller means past one hop.
	Types []string

	// Direction is which way to follow an edge. The zero value is [Outbound].
	Direction Direction

	// MinDepth is the shortest path to return. Zero and one both mean one hop.
	MinDepth int

	// MaxDepth is the longest path to follow. It is required by
	// [Graph.Traverse]: an unbounded walk on a connected graph visits
	// everything.
	MaxDepth int

	// Limit caps how many results come back. Zero takes the backend's default
	// rather than meaning unlimited, for the same reason a listing has a default
	// page size.
	Limit int

	// WithProps loads each edge's own fields. Off by default because a walk that
	// only needs to know what is connected should not pay to decode what the
	// connections say.
	WithProps bool
}

// Path is one route found by a walk: the records it passed through and the edges
// it followed.
//
// Vertices always has exactly one more entry than Edges, starting with the
// record the walk began at — so a caller can read the route without matching the
// two slices up itself.
type Path struct {
	Vertices []Ref
	Edges    []Edge
}

// Depth is how many hops this path covers.
func (p Path) Depth() int { return len(p.Edges) }

// End is the record the path arrived at, and reports false for a path that never
// left its starting point.
func (p Path) End() (Ref, bool) {
	if len(p.Vertices) == 0 {
		return Ref{}, false
	}
	return p.Vertices[len(p.Vertices)-1], len(p.Edges) > 0
}
