package arangodb

import (
	"context"
	"fmt"
	"strings"

	"github.com/arangodb/go-driver/v2/arangodb"
	arangoshared "github.com/arangodb/go-driver/v2/arangodb/shared"
	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database/core"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Driver is a store.Driver backed by ArangoDB.
//
// A resource is a collection, a record is a document, and the descriptor's
// columns are the document's fields — the same layout the MongoDB driver uses,
// because both stores hold JSON-shaped documents and a caller moving between
// them should find its data where it left it.
//
// What ArangoDB adds is the second half: the same server holds edges between
// those documents and can walk them. See [Driver.Connect] and the [store.Graph]
// capability, which is the reason this backend exists alongside MongoDB rather
// than instead of it.
type Driver struct {
	client arangodb.Client
	dbName string

	// registry turns a resource name into a collection, which the graph half
	// needs and the record half does not. Nil is fine for a program that only
	// stores records — see registry.go.
	registry *store.Registry

	// tx binds every operation to one stream transaction, and is nil outside
	// one. ArangoDB attaches a transaction to the database handle rather than to
	// a context, so this is what collection and query access go through when it
	// is set.
	tx arangodb.Transaction
}

var (
	_ store.Driver   = (*Driver)(nil)
	_ store.Migrator = (*Driver)(nil)
	_ store.Batcher  = (*Driver)(nil)
)

// New returns a driver over a client you own, writing every resource into the
// database its descriptor names.
//
// This package does not dial the client and does not close it. Use
// [NewProvider] to select a database at runtime instead.
func New(client *Client) *Driver { return &Driver{client: client.inner} }

// database opens the database this driver writes to.
func (d *Driver) database(ctx context.Context, res *store.Resource) (arangodb.Database, error) {
	name := d.dbName
	if name == "" && res != nil {
		name = res.Schema
	}
	if name == "" {
		return nil, fmt.Errorf("arangodb: no database selected and resource %q names none", resName(res))
	}
	db, err := d.client.GetDatabase(ctx, name, &arangodb.GetDatabaseOptions{SkipExistCheck: true})
	if err != nil {
		return nil, fmt.Errorf("arangodb: cannot open database %q: %w", name, err)
	}
	return db, nil
}

// collection opens the collection a resource lives in, inside this driver's
// transaction when it has one.
func (d *Driver) collection(ctx context.Context, res *store.Resource) (arangodb.Collection, error) {
	opts := &arangodb.GetCollectionOptions{SkipExistCheck: true}
	if d.tx != nil {
		coll, err := d.tx.GetCollection(ctx, res.Table, opts)
		if err != nil {
			return nil, fmt.Errorf("arangodb: cannot open collection %q in the transaction: %w", res.Table, err)
		}
		return coll, nil
	}
	db, err := d.database(ctx, res)
	if err != nil {
		return nil, err
	}
	coll, err := db.GetCollection(ctx, res.Table, opts)
	if err != nil {
		return nil, fmt.Errorf("arangodb: cannot open collection %q: %w", res.Table, err)
	}
	return coll, nil
}

// query runs AQL, inside this driver's transaction when it has one.
func (d *Driver) query(ctx context.Context, res *store.Resource, aql string, binds map[string]any) (arangodb.Cursor, error) {
	opts := &arangodb.QueryOptions{BindVars: binds}
	if d.tx != nil {
		return d.tx.Query(ctx, aql, opts)
	}
	db, err := d.database(ctx, res)
	if err != nil {
		return nil, err
	}
	return db.Query(ctx, aql, opts)
}

// Create stores msg as a new document.
func (d *Driver) Create(ctx context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	if res == nil {
		return store.WriteResult{}, fmt.Errorf("arangodb: Create needs a resource")
	}
	coll, err := d.collection(ctx, res)
	if err != nil {
		return store.WriteResult{}, err
	}
	cols, err := store.MessageToColumns(res, msg)
	if err != nil {
		return store.WriteResult{}, err
	}
	core.FillManaged(res, cols, true)

	doc, err := toDocument(res, cols)
	if err != nil {
		return store.WriteResult{}, err
	}
	if _, cerr := coll.CreateDocument(ctx, doc); cerr != nil {
		return store.WriteResult{}, translate(cerr, res, keyOf(doc))
	}

	out, err := store.ColumnsToMessage(res, cols)
	if err != nil {
		return store.WriteResult{}, err
	}
	return store.WriteResult{Message: out}, nil
}

// Get returns the document under key.
func (d *Driver) Get(ctx context.Context, res *store.Resource, key string) (proto.Message, error) {
	if res == nil {
		return nil, fmt.Errorf("arangodb: Get needs a resource")
	}
	coll, err := d.collection(ctx, res)
	if err != nil {
		return nil, err
	}
	var doc map[string]any
	if _, rerr := coll.ReadDocument(ctx, escapeKey(key), &doc); rerr != nil {
		return nil, translate(rerr, res, key)
	}
	return fromDocument(res, doc)
}

// Update replaces the document identified by msg's primary key.
//
// A replacement rather than a partial update, for the reason the contract
// gives: Update overwrites the record, and a merge would leave a field that used
// to have a value and no longer does exactly as it was.
func (d *Driver) Update(ctx context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	if res == nil {
		return store.WriteResult{}, fmt.Errorf("arangodb: Update needs a resource")
	}
	coll, err := d.collection(ctx, res)
	if err != nil {
		return store.WriteResult{}, err
	}
	key, err := store.KeyOf(res, msg)
	if err != nil {
		return store.WriteResult{}, err
	}
	cols, err := store.MessageToColumns(res, msg)
	if err != nil {
		return store.WriteResult{}, err
	}
	core.FillManaged(res, cols, false)

	doc, err := toDocument(res, cols)
	if err != nil {
		return store.WriteResult{}, err
	}
	if _, rerr := coll.ReplaceDocument(ctx, escapeKey(key), doc); rerr != nil {
		return store.WriteResult{}, translate(rerr, res, key)
	}

	out, err := store.ColumnsToMessage(res, cols)
	if err != nil {
		return store.WriteResult{}, err
	}
	return store.WriteResult{Message: out}, nil
}

// Delete removes the document under key.
func (d *Driver) Delete(ctx context.Context, res *store.Resource, key string) error {
	if res == nil {
		return fmt.Errorf("arangodb: Delete needs a resource")
	}
	coll, err := d.collection(ctx, res)
	if err != nil {
		return err
	}
	if _, derr := coll.DeleteDocument(ctx, escapeKey(key)); derr != nil {
		return translate(derr, res, key)
	}
	return nil
}

// Exists reports whether a document with the given key is there.
func (d *Driver) Exists(ctx context.Context, res *store.Resource, key string) (bool, error) {
	if res == nil {
		return false, fmt.Errorf("arangodb: Exists needs a resource")
	}
	coll, err := d.collection(ctx, res)
	if err != nil {
		return false, err
	}
	ok, derr := coll.DocumentExists(ctx, escapeKey(key))
	if derr != nil {
		return false, fmt.Errorf("arangodb: cannot check %s: %w", key, derr)
	}
	return ok, nil
}

// Count returns how many documents match opts.Filter.
func (d *Driver) Count(ctx context.Context, res *store.Resource, opts store.ListOptions) (int64, error) {
	if res == nil {
		return 0, fmt.Errorf("arangodb: Count needs a resource")
	}
	where, binds, err := buildFilter(res, opts.Filter)
	if err != nil {
		return 0, err
	}
	query := fmt.Sprintf("FOR doc IN @@coll %s COLLECT WITH COUNT INTO n RETURN n", where)
	binds["@coll"] = res.Table

	var n int64
	if qerr := d.queryOne(ctx, res, query, binds, &n); qerr != nil {
		return 0, qerr
	}
	return n, nil
}

// List returns a page of documents.
//
// Ordering, paging and filtering are all done by the server, in one AQL query.
// The filter is the same small subset every backend here accepts — see
// [buildFilter] — because a backend that took the whole grammar and honored part
// of it would return the wrong records with nothing to say so.
func (d *Driver) List(ctx context.Context, res *store.Resource, opts store.ListOptions) (store.ListResult, error) {
	if res == nil {
		return store.ListResult{}, fmt.Errorf("arangodb: List needs a resource")
	}
	total := core.NoTotal
	if !opts.OmitTotal {
		var cerr error
		if total, cerr = d.Count(ctx, res, opts); cerr != nil {
			return store.ListResult{}, cerr
		}
	}

	where, binds, err := buildFilter(res, opts.Filter)
	if err != nil {
		return store.ListResult{}, err
	}
	limit := core.PageSize(opts.PageSize)
	offset := core.DecodeToken(opts.PageToken)

	query := fmt.Sprintf("FOR doc IN @@coll %s SORT %s LIMIT @offset, @limit RETURN doc",
		where, sortClause(res, opts.OrderBy))
	binds["@coll"] = res.Table
	binds["offset"] = offset
	binds["limit"] = core.FetchLimit(limit, opts.OmitTotal)

	cur, err := d.query(ctx, res, query, binds)
	if err != nil {
		return store.ListResult{}, fmt.Errorf("arangodb: cannot list %s: %w", res.Table, err)
	}
	defer func() { _ = cur.Close() }()

	var items []proto.Message
	for cur.HasMore() {
		var doc map[string]any
		if _, rerr := cur.ReadDocument(ctx, &doc); rerr != nil {
			return store.ListResult{}, fmt.Errorf("arangodb: cannot read the %s cursor: %w", res.Name, rerr)
		}
		msg, merr := fromDocument(res, doc)
		if merr != nil {
			return store.ListResult{}, merr
		}
		items = append(items, msg)
	}

	items, next := core.TrimPage(items, offset, limit, total, opts.OmitTotal)
	return store.ListResult{Items: items, NextPageToken: next, Total: total}, nil
}

// queryOne runs a query expected to return a single value.
func (d *Driver) queryOne(ctx context.Context, res *store.Resource, aql string, binds map[string]any, dest any) error {
	cur, err := d.query(ctx, res, aql, binds)
	if err != nil {
		return fmt.Errorf("arangodb: query failed on %s: %w", res.Table, err)
	}
	defer func() { _ = cur.Close() }()

	if !cur.HasMore() {
		return nil
	}
	if _, rerr := cur.ReadDocument(ctx, dest); rerr != nil {
		return fmt.Errorf("arangodb: cannot read the result: %w", rerr)
	}
	return nil
}

// sortClause turns an AIP-132 order expression into an AQL SORT.
//
// The column name is checked against the descriptor rather than interpolated,
// so an expression arriving from a request cannot reach into the query. An
// unrecognized column sorts by key instead of failing: ordering is a
// presentation concern, and the right records in an unexpected order beat an
// error.
func sortClause(res *store.Resource, orderBy string) string {
	if orderBy == "" {
		return "doc._key ASC"
	}
	fields := strings.Fields(orderBy)
	direction := "ASC"
	if len(fields) > 1 && strings.EqualFold(fields[1], "desc") {
		direction = "DESC"
	}
	column := fields[0]
	if strings.EqualFold(column, res.PKColumn) {
		return "doc._key " + direction
	}
	col, ok := res.LookupColumn(column)
	if !ok {
		return "doc._key ASC"
	}
	return "doc." + col.Name + " " + direction
}

// translate turns a driver error into the contract's vocabulary.
func translate(err error, res *store.Resource, key string) error {
	switch {
	case arangoshared.IsNotFound(err):
		return fmt.Errorf("%w: %s", store.ErrNotFound, key)
	case arangoshared.IsConflict(err):
		return fmt.Errorf("%w: %s", store.ErrAlreadyExists, key)
	default:
		return fmt.Errorf("arangodb: %s: %w", resName(res), err)
	}
}

// resName is the resource's name, or a placeholder, for a message that has to
// say something either way.
func resName(res *store.Resource) string {
	if res == nil {
		return "<no resource>"
	}
	return res.Name
}

// keyOf reads the _key back out of a document being written, for an error
// message that can name what failed.
func keyOf(doc map[string]any) string {
	if k, ok := doc["_key"].(string); ok {
		return unescapeKey(k)
	}
	return ""
}
