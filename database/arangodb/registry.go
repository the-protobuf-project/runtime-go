package arangodb

import (
	"fmt"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Resolving a resource name to a collection, which is what the graph half needs
// and the record half does not.
//
// A [store.Ref] carries a resource name because that is the only identifier
// portable across backends — ArangoDB writes "collection/key" and Neo4j has no
// such notion. Turning that name back into a collection means holding the same
// registry the rest of the program uses, so a provider takes one.
//
// The record half needs none of this: every CRUD call is handed the descriptor
// it operates on. That is why the registry is an option rather than a
// requirement, and why a program that never touches the graph never has to
// supply it.

// resourceFor returns the descriptor registered under a resource name.
func (d *Driver) resourceFor(name string) (*store.Resource, error) {
	if d.registry == nil {
		return nil, fmt.Errorf(
			"arangodb: graph operations need a registry to turn a resource name into a collection; build the provider with WithRegistry")
	}
	res, err := d.registry.Resource(name)
	if err != nil {
		return nil, fmt.Errorf("arangodb: %w", err)
	}
	return res, nil
}

// resourceName is the reverse: the resource whose collection this is, falling
// back to the collection name where nothing claims it.
//
// A fallback rather than an error because this runs while decoding a traversal,
// and a walk that crossed into a collection the registry does not know is worth
// reporting as the name it found rather than failing the whole read.
func (d *Driver) resourceName(collection string) string {
	if d.registry == nil {
		return collection
	}
	for _, name := range d.registry.Names() {
		if res, err := d.registry.Resource(name); err == nil && res.Table == collection {
			return name
		}
	}
	return collection
}

// refFor turns a stored document id back into a [store.Ref].
func (d *Driver) refFor(documentID string) store.Ref {
	collection, key := splitDocumentID(documentID)
	return store.Ref{Resource: d.resourceName(collection), Key: key}
}

// tableNames maps resource names to the collections they live in.
func (d *Driver) tableNames(names []string) ([]string, error) {
	out := make([]string, 0, len(names))
	for _, name := range names {
		res, err := d.resourceFor(name)
		if err != nil {
			return nil, err
		}
		out = append(out, res.Table)
	}
	return out, nil
}

// edgeCollections resolves the edge types a walk may follow.
//
// An empty list means every edge collection the registry knows, which is the
// only honest reading of "follow every type" — AQL needs the collections named,
// so there is no wildcard to pass through.
func (d *Driver) edgeCollections(types []string) ([]string, error) {
	if len(types) > 0 {
		return d.tableNames(types)
	}
	if d.registry == nil {
		return nil, fmt.Errorf(
			"arangodb: a traversal with no Types needs a registry to know which edge collections exist")
	}

	var out []string
	for _, name := range d.registry.Names() {
		res, err := d.registry.Resource(name)
		if err != nil || !res.IsEdge {
			continue
		}
		out = append(out, res.Table)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf(
			"arangodb: no edge resources are registered; mark an edge resource with Resource.IsEdge or name the types to follow")
	}
	return out, nil
}
