package store_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// bare implements the CRUD contract and nothing else, standing in for a backend
// with no transactions, no migrations and no graph.
type bare struct{ store.Driver }

// A transaction must reach everything the database can do, not only the CRUD
// half — the reason Run hands over a *DB.
func TestTransactionBodySeesEveryCapability(t *testing.T) {
	db := store.Build(bare{}, "fake", "test", nil)
	ctx := context.Background()

	err := db.Tx.Run(ctx, func(tx *store.DB) error {
		t.Fatal("a backend with no transactions must not run the body")
		return nil
	})
	if !errors.Is(err, store.ErrUnimplemented) {
		t.Fatalf("Tx.Run = %v, want ErrUnimplemented", err)
	}
	if !strings.Contains(err.Error(), "fake") {
		t.Errorf("the refusal does not name the backend: %v", err)
	}
}

// Every capability field is non-nil on every backend, so a wiring mistake
// surfaces as a refusal naming the backend rather than as a nil dereference on
// a line that looks innocent.
func TestCapabilityFieldsAreNeverNil(t *testing.T) {
	db := store.Build(bare{}, "fake", "test", nil)
	ctx := context.Background()

	if db.Tx == nil || db.Schema == nil || db.Graph == nil {
		t.Fatal("a capability field is nil")
	}

	if _, err := db.Graph.Connect(ctx, nil, store.Ref{}, store.Ref{}, nil); !errors.Is(err, store.ErrUnimplemented) {
		t.Errorf("Graph.Connect = %v, want ErrUnimplemented", err)
	}
	if _, err := db.Graph.Neighbors(ctx, store.Ref{}, store.TraverseOptions{}); !errors.Is(err, store.ErrUnimplemented) {
		t.Errorf("Graph.Neighbors = %v, want ErrUnimplemented", err)
	}
	if _, err := db.Graph.Traverse(ctx, store.Ref{}, store.TraverseOptions{}); !errors.Is(err, store.ErrUnimplemented) {
		t.Errorf("Graph.Traverse = %v, want ErrUnimplemented", err)
	}
	if err := db.Graph.Disconnect(ctx, nil, "x"); !errors.Is(err, store.ErrUnimplemented) {
		t.Errorf("Graph.Disconnect = %v, want ErrUnimplemented", err)
	}
	if err := db.Schema.EnsureSchema(ctx, nil); !errors.Is(err, store.ErrUnimplemented) {
		t.Errorf("Schema.EnsureSchema = %v, want ErrUnimplemented", err)
	}
}

// A graph refusal must say it is not a graph, rather than repeating the generic
// message — the point of naming the backend is that the reader learns which one.
func TestGraphRefusalNamesTheBackend(t *testing.T) {
	db := store.Build(bare{}, "postgres", "", nil)
	_, err := db.Graph.Neighbors(context.Background(), store.Ref{}, store.TraverseOptions{})
	if err == nil || !strings.Contains(err.Error(), "postgres") {
		t.Errorf("refusal = %v, want it to name postgres", err)
	}
	if !strings.Contains(err.Error(), "not a graph") {
		t.Errorf("refusal = %v, want it to say why", err)
	}
}

func TestPathReadsItsOwnRoute(t *testing.T) {
	p := store.Path{
		Vertices: []store.Ref{
			{Resource: "User", Key: "a"},
			{Resource: "User", Key: "b"},
			{Resource: "Org", Key: "acme"},
		},
		Edges: []store.Edge{
			{Type: "Follows"}, {Type: "MemberOf"},
		},
	}
	if p.Depth() != 2 {
		t.Errorf("Depth = %d, want 2", p.Depth())
	}
	end, ok := p.End()
	if !ok || end.Key != "acme" {
		t.Errorf("End = %v, %v; want acme", end, ok)
	}

	// A path that never left where it started reports that, rather than
	// pretending its origin was a destination.
	origin := store.Path{Vertices: []store.Ref{{Resource: "User", Key: "a"}}}
	if _, moved := origin.End(); moved {
		t.Error("a zero-hop path reported that it arrived somewhere")
	}
}

func TestEndsAndStarts(t *testing.T) {
	edges := []store.Edge{
		{From: store.Ref{Resource: "User", Key: "a"}, To: store.Ref{Resource: "User", Key: "b"}},
		{From: store.Ref{Resource: "User", Key: "a"}, To: store.Ref{Resource: "User", Key: "c"}},
	}
	ends := store.Ends(edges)
	if len(ends) != 2 || ends[0].Key != "b" || ends[1].Key != "c" {
		t.Errorf("Ends = %v", ends)
	}
	starts := store.Starts(edges)
	if len(starts) != 2 || starts[0].Key != "a" {
		t.Errorf("Starts = %v", starts)
	}
}

func TestResolveNeedsARegistry(t *testing.T) {
	_, err := store.Resolve[proto.Message](context.Background(), bare{}, nil, nil)
	if err == nil {
		t.Fatal("Resolve without a registry was accepted")
	}
	if !strings.Contains(err.Error(), "registry") {
		t.Errorf("the error does not say what is missing: %v", err)
	}
}

// A ref naming a resource nobody registered means the graph and the registry
// disagree about what exists — an error, not a gap to skip quietly.
func TestResolveRefusesAnUnknownResource(t *testing.T) {
	reg := store.NewRegistry()
	_, err := store.Resolve[proto.Message](context.Background(), bare{}, reg,
		[]store.Ref{{Resource: "Ghost", Key: "x"}})
	if err == nil {
		t.Fatal("Resolve accepted a ref to an unregistered resource")
	}
	if !strings.Contains(err.Error(), "Ghost") {
		t.Errorf("the error does not name the resource: %v", err)
	}
}
