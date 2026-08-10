package arangodb

import (
	"context"
	"fmt"

	"github.com/arangodb/go-driver/v2/arangodb/shared"

	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database/core"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

// batchSize is how many keys go into one bulk read.
const batchSize = 512

// CreateMany inserts every message in one request.
//
// It stops at the first failure and returns the results written before it, which
// is what the contract's one-result-per-message shape allows a caller to act on:
// a gap in the middle has no way to say which message it belonged to.
func (d *Driver) CreateMany(ctx context.Context, res *store.Resource, msgs []proto.Message) ([]store.WriteResult, error) {
	if res == nil {
		return nil, fmt.Errorf("arangodb: CreateMany needs a resource")
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	coll, err := d.collection(ctx, res)
	if err != nil {
		return nil, err
	}

	docs := make([]map[string]any, 0, len(msgs))
	out := make([]store.WriteResult, 0, len(msgs))
	for i, msg := range msgs {
		cols, cerr := store.MessageToColumns(res, msg)
		if cerr != nil {
			return nil, fmt.Errorf("arangodb: CreateMany stopped at index %d: %w", i, cerr)
		}
		core.FillManaged(res, cols, true)
		doc, derr := toDocument(res, cols)
		if derr != nil {
			return nil, fmt.Errorf("arangodb: CreateMany stopped at index %d: %w", i, derr)
		}
		filled, merr := store.ColumnsToMessage(res, cols)
		if merr != nil {
			return nil, fmt.Errorf("arangodb: CreateMany stopped at index %d: %w", i, merr)
		}
		docs = append(docs, doc)
		out = append(out, store.WriteResult{Message: filled})
	}

	reader, err := coll.CreateDocuments(ctx, docs)
	if err != nil {
		return nil, fmt.Errorf("arangodb: cannot insert into %s: %w", res.Table, err)
	}

	// The driver reports per-document outcomes through the reader rather than as
	// one error, so a partial failure is discovered here. Exhaustion and failure
	// both end the loop and they are not the same thing: reading no further
	// because the batch is done is success, and reading no further because the
	// connection went is not.
	written := 0
	for {
		meta, rerr := reader.Read()
		if shared.IsNoMoreDocuments(rerr) {
			break
		}
		if rerr != nil {
			return out[:written], fmt.Errorf("arangodb: cannot read the insert result: %w", rerr)
		}
		if meta.Error != nil && *meta.Error {
			return out[:written], fmt.Errorf("%w: %s at index %d",
				store.ErrAlreadyExists, res.Name, written)
		}
		written++
	}
	return out, nil
}

// GetMany returns the documents for the given keys, in the order asked.
//
// One query per batch rather than one read per key, with the results put back in
// order afterwards. A nil entry is a document that was not there, which is
// ordinary for a bulk read racing a delete.
func (d *Driver) GetMany(ctx context.Context, res *store.Resource, keys []string) ([]proto.Message, error) {
	if res == nil {
		return nil, fmt.Errorf("arangodb: GetMany needs a resource")
	}
	if len(keys) == 0 {
		return nil, nil
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
		escaped := make([]string, 0, end-start)
		for _, k := range keys[start:end] {
			escaped = append(escaped, escapeKey(k))
		}

		const aql = "FOR doc IN @@coll FILTER doc._key IN @keys RETURN doc"
		cur, err := d.query(ctx, res, aql, map[string]any{"@coll": res.Table, "keys": escaped})
		if err != nil {
			return nil, fmt.Errorf("arangodb: cannot read %s: %w", res.Table, err)
		}
		for cur.HasMore() {
			var doc map[string]any
			if _, rerr := cur.ReadDocument(ctx, &doc); rerr != nil {
				_ = cur.Close()
				return nil, fmt.Errorf("arangodb: cannot read the %s cursor: %w", res.Name, rerr)
			}
			msg, merr := fromDocument(res, doc)
			if merr != nil {
				_ = cur.Close()
				return nil, merr
			}
			key, kerr := store.KeyOf(res, msg)
			if kerr != nil {
				_ = cur.Close()
				return nil, kerr
			}
			if i, ok := at[key]; ok {
				out[i] = msg
			}
		}
		_ = cur.Close()
	}
	return out, nil
}
