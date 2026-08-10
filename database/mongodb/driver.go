package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database/core"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Driver is a store.Driver backed by MongoDB.
//
// A resource is a collection, a record is a document, and the descriptor's
// columns are the document's fields — so what lands in MongoDB is queryable
// with the tools people already use, rather than an opaque blob only this
// program can read. That is the whole reason to use MongoDB over a key-value
// store, and encoding a message as one binary field would throw it away.
//
// # The primary key is _id
//
// MongoDB gives every document an _id and indexes it. The descriptor's primary
// key is stored there rather than in a field beside it: two ideas of identity in
// one document is how a store ends up with a record that can be found one way
// and not the other. The column keeps its own name in the descriptor and is
// mapped on the way in and out.
type Driver struct {
	client *mongo.Client
	db     string
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
func New(client *mongo.Client) *Driver { return &Driver{client: client} }

// collection returns the collection a resource lives in.
//
// A database selected at runtime wins over the one the descriptor was generated
// with: the descriptor says what a Book is, the selection says whose.
func (d *Driver) collection(res *store.Resource) (*mongo.Collection, error) {
	db := d.db
	if db == "" {
		db = res.Schema
	}
	if db == "" {
		return nil, fmt.Errorf("mongodb: resource %q names no database and none was selected", res.Name)
	}
	return d.client.Database(db).Collection(res.Table), nil
}

// Create stores msg as a new document.
//
// A duplicate primary key is reported as [store.ErrAlreadyExists] rather than as
// a driver error, and so is any other unique index the descriptor declared —
// both arrive as the same write exception and both mean the same thing to a
// caller.
func (d *Driver) Create(ctx context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	if res == nil {
		return store.WriteResult{}, fmt.Errorf("mongodb: Create needs a resource")
	}
	coll, err := d.collection(res)
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
	if _, ierr := coll.InsertOne(ctx, doc); ierr != nil {
		if mongo.IsDuplicateKeyError(ierr) {
			return store.WriteResult{}, fmt.Errorf("%w: %s", store.ErrAlreadyExists, res.Name)
		}
		return store.WriteResult{}, fmt.Errorf("mongodb: cannot insert into %s: %w", res.Table, ierr)
	}

	// The message the caller gets back carries whatever the driver supplied — a
	// generated key, the audit timestamps — rather than what it handed in.
	out, err := store.ColumnsToMessage(res, cols)
	if err != nil {
		return store.WriteResult{}, err
	}
	return store.WriteResult{Message: out}, nil
}

// Get returns the document under key.
func (d *Driver) Get(ctx context.Context, res *store.Resource, key string) (proto.Message, error) {
	if res == nil {
		return nil, fmt.Errorf("mongodb: Get needs a resource")
	}
	coll, err := d.collection(res)
	if err != nil {
		return nil, err
	}
	var doc bson.M
	if err := coll.FindOne(ctx, bson.M{"_id": key}).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("%w: %s", store.ErrNotFound, key)
		}
		return nil, fmt.Errorf("mongodb: cannot read %s: %w", key, err)
	}
	return fromDocument(res, doc)
}

// Update replaces the document identified by msg's primary key.
//
// A replacement rather than a field-by-field update, because the contract says
// Update overwrites the record: a $set of the columns present would leave a
// field that used to have a value and no longer does exactly as it was, which
// reads as the write having silently half-applied.
func (d *Driver) Update(ctx context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	if res == nil {
		return store.WriteResult{}, fmt.Errorf("mongodb: Update needs a resource")
	}
	coll, err := d.collection(res)
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
	result, err := coll.ReplaceOne(ctx, bson.M{"_id": key}, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return store.WriteResult{}, fmt.Errorf("%w: %s", store.ErrAlreadyExists, key)
		}
		return store.WriteResult{}, fmt.Errorf("mongodb: cannot update %s: %w", key, err)
	}
	if result.MatchedCount == 0 {
		return store.WriteResult{}, fmt.Errorf("%w: %s", store.ErrNotFound, key)
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
		return fmt.Errorf("mongodb: Delete needs a resource")
	}
	coll, err := d.collection(res)
	if err != nil {
		return err
	}
	result, err := coll.DeleteOne(ctx, bson.M{"_id": key})
	if err != nil {
		return fmt.Errorf("mongodb: cannot delete %s: %w", key, err)
	}
	if result.DeletedCount == 0 {
		return fmt.Errorf("%w: %s", store.ErrNotFound, key)
	}
	return nil
}

// Exists reports whether a document with the given key is there.
//
// A projection of nothing but _id, so the answer costs an index lookup rather
// than transferring a document to throw it away.
func (d *Driver) Exists(ctx context.Context, res *store.Resource, key string) (bool, error) {
	if res == nil {
		return false, fmt.Errorf("mongodb: Exists needs a resource")
	}
	coll, err := d.collection(res)
	if err != nil {
		return false, err
	}
	err = coll.FindOne(ctx, bson.M{"_id": key},
		mongoFindOneIDOnly()).Err()
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, mongo.ErrNoDocuments):
		return false, nil
	default:
		return false, fmt.Errorf("mongodb: cannot check %s: %w", key, err)
	}
}

// Count returns how many documents match opts.Filter.
func (d *Driver) Count(ctx context.Context, res *store.Resource, opts store.ListOptions) (int64, error) {
	if res == nil {
		return 0, fmt.Errorf("mongodb: Count needs a resource")
	}
	coll, err := d.collection(res)
	if err != nil {
		return 0, err
	}
	filter, err := parseFilter(res, opts.Filter)
	if err != nil {
		return 0, err
	}
	n, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("mongodb: cannot count %s: %w", res.Table, err)
	}
	return n, nil
}

// List returns a page of documents.
//
// Ordering, paging and filtering are all done by the server. The filter is a
// deliberately small subset of AIP-160 — see [parseFilter] — because accepting
// the whole grammar and honoring part of it is worse than accepting a little and
// honoring all of it.
func (d *Driver) List(ctx context.Context, res *store.Resource, opts store.ListOptions) (store.ListResult, error) {
	if res == nil {
		return store.ListResult{}, fmt.Errorf("mongodb: List needs a resource")
	}
	coll, err := d.collection(res)
	if err != nil {
		return store.ListResult{}, err
	}
	filter, err := parseFilter(res, opts.Filter)
	if err != nil {
		return store.ListResult{}, err
	}

	total := core.NoTotal
	if !opts.OmitTotal {
		var cerr error
		if total, cerr = coll.CountDocuments(ctx, filter); cerr != nil {
			return store.ListResult{}, fmt.Errorf("mongodb: cannot count %s: %w", res.Table, cerr)
		}
	}

	limit := core.PageSize(opts.PageSize)
	offset := core.DecodeToken(opts.PageToken)

	find := mongoFindPage(sortOf(res, opts.OrderBy), core.FetchLimit(limit, opts.OmitTotal), offset)
	cur, err := coll.Find(ctx, filter, find)
	if err != nil {
		return store.ListResult{}, fmt.Errorf("mongodb: cannot list %s: %w", res.Table, err)
	}
	defer func() { _ = cur.Close(ctx) }()

	var items []proto.Message
	for cur.Next(ctx) {
		var doc bson.M
		if derr := cur.Decode(&doc); derr != nil {
			return store.ListResult{}, fmt.Errorf("mongodb: cannot decode a %s: %w", res.Name, derr)
		}
		msg, merr := fromDocument(res, doc)
		if merr != nil {
			return store.ListResult{}, merr
		}
		items = append(items, msg)
	}
	if cerr := cur.Err(); cerr != nil {
		return store.ListResult{}, fmt.Errorf("mongodb: cannot read the %s cursor: %w", res.Name, cerr)
	}

	items, next := core.TrimPage(items, offset, limit, total, opts.OmitTotal)
	return store.ListResult{Items: items, NextPageToken: next, Total: total}, nil
}

// sortOf turns an AIP-132 order expression into a Mongo sort.
//
// An unrecognized column sorts by _id instead of failing: ordering is a
// presentation concern, and a listing that returns the right records in an
// unexpected order is more useful than one that returns an error.
func sortOf(res *store.Resource, orderBy string) bson.D {
	if orderBy == "" {
		return bson.D{{Key: "_id", Value: 1}}
	}
	fields := strings.Fields(orderBy)
	column := fields[0]
	direction := 1
	if len(fields) > 1 && strings.EqualFold(fields[1], "desc") {
		direction = -1
	}
	if strings.EqualFold(column, res.PKColumn) {
		return bson.D{{Key: "_id", Value: direction}}
	}
	if _, ok := res.LookupColumn(column); !ok {
		return bson.D{{Key: "_id", Value: 1}}
	}
	return bson.D{{Key: column, Value: direction}}
}
