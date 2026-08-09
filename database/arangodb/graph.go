package arangodb

import (
	"context"
	"fmt"
	"strings"

	"github.com/arangodb/go-driver/v2/arangodb"
	arangoshared "github.com/arangodb/go-driver/v2/arangodb/shared"
	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/database/core"
)

var (
	_ database.Graph         = (*Driver)(nil)
	_ database.GraphMigrator = (*Driver)(nil)
)

// defaultTraverseLimit bounds a walk that names none, for the same reason a
// listing has a default page size: a caller who forgot is better served by a
// page than by everything.
const defaultTraverseLimit = 100

// Connect creates an edge from one record to another.
//
// Both endpoints are checked before the edge is written. ArangoDB will happily
// store an edge pointing at a document that does not exist — _from and _to are
// strings to it — and a dangling edge is the kind of wrong that surfaces much
// later as a traversal returning nothing for a record that is plainly there.
func (d *Driver) Connect(ctx context.Context, edge *database.Resource, from, to database.Ref, props proto.Message) (database.Edge, error) {
	if edge == nil {
		return database.Edge{}, fmt.Errorf("arangodb: Connect needs an edge resource")
	}
	fromID, err := d.resolveRef(ctx, from)
	if err != nil {
		return database.Edge{}, err
	}
	toID, err := d.resolveRef(ctx, to)
	if err != nil {
		return database.Edge{}, err
	}

	doc := map[string]any{fieldFrom: fromID, fieldTo: toID}
	if props != nil {
		cols, cerr := database.MessageToColumns(edge, props)
		if cerr != nil {
			return database.Edge{}, cerr
		}
		core.FillManaged(edge, cols, true)
		// An edge's key belongs to the server, not the caller: Connect returns
		// the one it assigned. A descriptor still names a primary-key column so
		// the edge can be read back like any other record, but an unset one here
		// is the normal case rather than a missing value.
		if key, _ := cols[edge.PKColumn].(string); key == "" {
			delete(cols, edge.PKColumn)
		}
		body, derr := toDocument(edge, cols)
		if derr != nil {
			return database.Edge{}, derr
		}
		for k, v := range body {
			doc[k] = v
		}
	}

	coll, err := d.collection(ctx, edge)
	if err != nil {
		return database.Edge{}, err
	}
	meta, err := coll.CreateDocument(ctx, doc)
	if err != nil {
		return database.Edge{}, translate(err, edge, "")
	}

	return database.Edge{
		Type:  edge.Name,
		Key:   unescapeKey(meta.Key),
		From:  from,
		To:    to,
		Props: props,
	}, nil
}

// Disconnect removes one edge by its key.
func (d *Driver) Disconnect(ctx context.Context, edge *database.Resource, key string) error {
	if edge == nil {
		return fmt.Errorf("arangodb: Disconnect needs an edge resource")
	}
	coll, err := d.collection(ctx, edge)
	if err != nil {
		return err
	}
	if _, derr := coll.DeleteDocument(ctx, escapeKey(key)); derr != nil {
		return translate(derr, edge, key)
	}
	return nil
}

// Neighbors returns the edges one hop from a record.
func (d *Driver) Neighbors(ctx context.Context, from database.Ref, opts database.TraverseOptions) ([]database.Edge, error) {
	o := opts
	if o.MaxDepth == 0 {
		o.MaxDepth = 1
	}
	paths, err := d.Traverse(ctx, from, o)
	if err != nil {
		return nil, err
	}
	edges := make([]database.Edge, 0, len(paths))
	for _, p := range paths {
		if len(p.Edges) > 0 {
			edges = append(edges, p.Edges[len(p.Edges)-1])
		}
	}
	return edges, nil
}

// Traverse walks outward from a record and returns the paths it found.
//
// The walk runs on the server as one AQL traversal, so a five-hop question costs
// one round trip rather than a fan-out per level — which is the reason to hold
// this data in a graph store rather than to join it in the application.
func (d *Driver) Traverse(ctx context.Context, from database.Ref, opts database.TraverseOptions) ([]database.Path, error) {
	res, err := d.resourceFor(from.Resource)
	if err != nil {
		return nil, err
	}
	startID := documentID(res.Table, from.Key)

	if opts.MaxDepth <= 0 {
		return nil, fmt.Errorf(
			"arangodb: Traverse needs a MaxDepth; an unbounded walk on a connected graph visits everything")
	}
	minDepth := max(opts.MinDepth, 1)
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultTraverseLimit
	}

	// Edge collections are named rather than bound, because AQL takes them as
	// part of the traversal clause and not as a value. Every one is resolved
	// through the registry first, so a name arriving from a request cannot
	// reach the query.
	collections, err := d.edgeCollections(opts.Types)
	if err != nil {
		return nil, err
	}

	aql := fmt.Sprintf(
		"FOR v, e, p IN %d..%d %s @start %s LIMIT @limit RETURN {vertices: p.vertices, edges: p.edges}",
		minDepth, opts.MaxDepth, aqlDirection(opts.Direction), strings.Join(collections, ", "))

	cur, err := d.query(ctx, res, aql, map[string]any{"start": startID, "limit": limit})
	if err != nil {
		return nil, fmt.Errorf("arangodb: cannot traverse from %s: %w", startID, err)
	}
	defer func() { _ = cur.Close() }()

	var out []database.Path
	for cur.HasMore() {
		var raw struct {
			Vertices []map[string]any `json:"vertices"`
			Edges    []map[string]any `json:"edges"`
		}
		if _, rerr := cur.ReadDocument(ctx, &raw); rerr != nil {
			return nil, fmt.Errorf("arangodb: cannot read the traversal cursor: %w", rerr)
		}
		path, perr := d.decodePath(raw.Vertices, raw.Edges, opts.WithProps)
		if perr != nil {
			return nil, perr
		}
		out = append(out, path)
	}
	return out, nil
}

// decodePath turns one AQL path into the contract's shape.
func (d *Driver) decodePath(vertices, edges []map[string]any, withProps bool) (database.Path, error) {
	path := database.Path{
		Vertices: make([]database.Ref, 0, len(vertices)),
		Edges:    make([]database.Edge, 0, len(edges)),
	}
	for _, v := range vertices {
		id, _ := v[fieldID].(string)
		path.Vertices = append(path.Vertices, d.refFor(id))
	}
	for _, e := range edges {
		id, _ := e[fieldID].(string)
		collection, key := splitDocumentID(id)
		fromID, _ := e[fieldFrom].(string)
		toID, _ := e[fieldTo].(string)

		edge := database.Edge{
			Type: d.resourceName(collection),
			Key:  key,
			From: d.refFor(fromID),
			To:   d.refFor(toID),
		}
		if withProps {
			if res, err := d.resourceFor(edge.Type); err == nil {
				if msg, merr := fromDocument(res, e); merr == nil {
					edge.Props = msg
				}
			}
		}
		path.Edges = append(path.Edges, edge)
	}
	return path, nil
}

// resolveRef turns a Ref into the document id an edge endpoint holds, checking
// the record is really there.
func (d *Driver) resolveRef(ctx context.Context, ref database.Ref) (string, error) {
	res, err := d.resourceFor(ref.Resource)
	if err != nil {
		return "", err
	}
	ok, err := d.Exists(ctx, res, ref.Key)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("%w: %s/%s", database.ErrNotFound, ref.Resource, ref.Key)
	}
	return documentID(res.Table, ref.Key), nil
}

// EnsureGraph declares a named graph over the given edge definitions, and is
// safe to call repeatedly.
//
// ArangoDB needs this and Neo4j does not, which is why it is a capability of its
// own rather than part of [database.Graph]. The edge collections are created if
// they are not there — a graph definition naming one that does not exist is
// refused by the server, and creating it here is what makes this callable on
// startup.
func (d *Driver) EnsureGraph(ctx context.Context, name string, defs []database.EdgeDefinition) error {
	if name == "" {
		return fmt.Errorf("arangodb: EnsureGraph needs a name")
	}
	db, err := d.database(ctx, nil)
	if err != nil {
		return err
	}

	edgeDefs := make([]arangodb.EdgeDefinition, 0, len(defs))
	for _, def := range defs {
		edgeRes, rerr := d.resourceFor(def.Edge)
		if rerr != nil {
			return rerr
		}
		from, ferr := d.tableNames(def.From)
		if ferr != nil {
			return ferr
		}
		to, terr := d.tableNames(def.To)
		if terr != nil {
			return terr
		}
		edgeDefs = append(edgeDefs, arangodb.EdgeDefinition{
			Collection: edgeRes.Table,
			From:       from,
			To:         to,
		})
	}

	if _, cerr := db.CreateGraph(ctx, name, &arangodb.GraphDefinition{
		EdgeDefinitions: edgeDefs,
	}, nil); cerr != nil && !arangoshared.IsConflict(cerr) {
		return fmt.Errorf("arangodb: cannot create graph %q: %w", name, cerr)
	}
	return nil
}

// DropGraph removes a graph declaration.
//
// The declaration only: the vertex and edge collections stay, which is
// ArangoDB's own default and the safer one. Dropping the data is
// [database.Migrator.DropSchema] on each collection, where it is visible in the
// calling code rather than implied by removing a definition.
func (d *Driver) DropGraph(ctx context.Context, name string) error {
	db, err := d.database(ctx, nil)
	if err != nil {
		return err
	}
	graph, gerr := db.Graph(ctx, name, nil)
	if gerr != nil {
		if arangoshared.IsNotFound(gerr) {
			return nil
		}
		return fmt.Errorf("arangodb: cannot open graph %q: %w", name, gerr)
	}
	if rerr := graph.Remove(ctx, nil); rerr != nil {
		return fmt.Errorf("arangodb: cannot drop graph %q: %w", name, rerr)
	}
	return nil
}

// aqlDirection renders a walk direction.
func aqlDirection(dir database.Direction) string {
	switch dir {
	case database.Inbound:
		return "INBOUND"
	case database.AnyDirection:
		return "ANY"
	default:
		return "OUTBOUND"
	}
}
