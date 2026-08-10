package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database/core"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

// batchSize is how many keys go into one bulk read.
const batchSize = 512

// CreateMany inserts every message in one round trip.
//
// Ordered, so it stops at the first failure and the results returned are the
// ones that were written. An unordered insert would be faster and would report a
// partial success no caller could act on: with the contract returning one result
// per message in order, a gap in the middle has no way to say which message it
// belonged to.
func (d *Driver) CreateMany(ctx context.Context, res *store.Resource, msgs []proto.Message) ([]store.WriteResult, error) {
	if res == nil {
		return nil, fmt.Errorf("mongodb: CreateMany needs a resource")
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	coll, err := d.collection(res)
	if err != nil {
		return nil, err
	}

	docs := make([]any, 0, len(msgs))
	out := make([]store.WriteResult, 0, len(msgs))
	for i, msg := range msgs {
		cols, cerr := store.MessageToColumns(res, msg)
		if cerr != nil {
			return nil, fmt.Errorf("mongodb: CreateMany stopped at index %d: %w", i, cerr)
		}
		core.FillManaged(res, cols, true)
		doc, derr := toDocument(res, cols)
		if derr != nil {
			return nil, fmt.Errorf("mongodb: CreateMany stopped at index %d: %w", i, derr)
		}
		filled, merr := store.ColumnsToMessage(res, cols)
		if merr != nil {
			return nil, fmt.Errorf("mongodb: CreateMany stopped at index %d: %w", i, merr)
		}
		docs = append(docs, doc)
		out = append(out, store.WriteResult{Message: filled})
	}

	result, err := coll.InsertMany(ctx, docs)
	if err != nil {
		written := 0
		if result != nil {
			written = len(result.InsertedIDs)
		}
		if mongo.IsDuplicateKeyError(err) {
			return out[:written], fmt.Errorf("%w: %s at index %d", store.ErrAlreadyExists, res.Name, written)
		}
		return out[:written], fmt.Errorf("mongodb: cannot insert into %s: %w", res.Table, err)
	}
	return out, nil
}

// GetMany returns the documents for the given keys, in the order asked.
//
// One query per batch rather than one per key, with the results put back in
// order afterwards — a $in returns them in whatever order the index walked. A
// nil entry is a document that was not there, which is ordinary for a bulk read
// racing a delete.
func (d *Driver) GetMany(ctx context.Context, res *store.Resource, keys []string) ([]proto.Message, error) {
	if res == nil {
		return nil, fmt.Errorf("mongodb: GetMany needs a resource")
	}
	if len(keys) == 0 {
		return nil, nil
	}
	coll, err := d.collection(res)
	if err != nil {
		return nil, err
	}

	at := make(map[string]int, len(keys))
	for i, key := range keys {
		if _, seen := at[key]; !seen {
			at[key] = i
		}
	}
	out := make([]proto.Message, len(keys))

	for start := 0; start < len(keys); start += batchSize {
		end := min(start+batchSize, len(keys))
		cur, ferr := coll.Find(ctx, bson.M{"_id": bson.M{"$in": keys[start:end]}})
		if ferr != nil {
			return nil, fmt.Errorf("mongodb: cannot read %s: %w", res.Table, ferr)
		}
		for cur.Next(ctx) {
			var doc bson.M
			if derr := cur.Decode(&doc); derr != nil {
				_ = cur.Close(ctx)
				return nil, fmt.Errorf("mongodb: cannot decode a %s: %w", res.Name, derr)
			}
			msg, merr := fromDocument(res, doc)
			if merr != nil {
				_ = cur.Close(ctx)
				return nil, merr
			}
			key, kerr := store.KeyOf(res, msg)
			if kerr != nil {
				_ = cur.Close(ctx)
				return nil, kerr
			}
			if i, ok := at[key]; ok {
				out[i] = msg
			}
		}
		cerr := cur.Err()
		_ = cur.Close(ctx)
		if cerr != nil {
			return nil, fmt.Errorf("mongodb: cannot read the %s cursor: %w", res.Name, cerr)
		}
	}
	return out, nil
}
