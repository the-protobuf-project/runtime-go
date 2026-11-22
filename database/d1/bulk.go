package d1

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// batchSize is how many inserts go into one request. D1 bounds a request by
// time — thirty seconds for the whole batch — rather than by count, so this is
// small enough to stay well inside it and large enough that the round trips are
// not the cost.
const batchSize = 50

// CreateMany stores every message, a batch of inserts per request.
//
// Each request is one unit: a duplicate anywhere in it fails all of it, and the
// error does not say which record it was. Requests before the failing one have
// already landed.
func (d *Driver) CreateMany(ctx context.Context, res *store.Resource, msgs []proto.Message) ([]store.WriteResult, error) {
	results := make([]store.WriteResult, 0, len(msgs))
	for start := 0; start < len(msgs); start += batchSize {
		chunk := msgs[start:min(start+batchSize, len(msgs))]
		stmts := make([]Statement, len(chunk))
		stored := make([]map[string]any, len(chunk))
		for i, msg := range chunk {
			s, cols, err := insert(res, msg)
			if err != nil {
				return results, err
			}
			stmts[i], stored[i] = s, cols
		}
		if _, err := d.runBatch(ctx, stmts); err != nil {
			return results, err
		}
		for _, cols := range stored {
			out, err := store.ColumnsToMessage(res, cols)
			if err != nil {
				return results, err
			}
			results = append(results, store.WriteResult{Message: out})
		}
	}
	return results, nil
}

// GetMany returns the records for the given keys, in the order asked, with nil
// where a record was not there. Keys go in pieces of at most [maxParams], the
// most D1 binds into one statement.
func (d *Driver) GetMany(ctx context.Context, res *store.Resource, keys []string) ([]proto.Message, error) {
	found := make(map[string]proto.Message, len(keys))
	for start := 0; start < len(keys); start += maxParams {
		chunk := keys[start:min(start+maxParams, len(keys))]
		out, err := d.run(ctx, Statement{
			SQL: fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s)",
				quote(res.Table), quote(res.PKColumn), placeholders(len(chunk))),
			Args: chunk,
		})
		if err != nil {
			return nil, err
		}
		for _, row := range out.Rows {
			key := fmt.Sprint(row[res.PKColumn])
			m, merr := message(res, row)
			if merr != nil {
				return nil, merr
			}
			found[key] = m
		}
	}
	items := make([]proto.Message, len(keys))
	for i, key := range keys {
		items[i] = found[key]
	}
	return items, nil
}
