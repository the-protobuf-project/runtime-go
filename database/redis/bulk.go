package redis

import (
	"context"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database"
)

// batchSize is how many keys go into one pipeline. Large enough that the
// per-trip overhead disappears, small enough that one slow reply does not hold
// a megabyte of results hostage.
const batchSize = 256

// GetMany fetches many records in one round trip per batch.
//
// This is the difference between listing a thousand records costing a thousand
// network hops and costing four. A pipeline rather than MGET, because MGET needs
// every key in one hash slot and a cluster client would refuse the call; the
// driver splits a pipeline across nodes on its own.
//
// A nil entry is a record that was not there — not an error, because a listing
// racing a delete is ordinary and the caller decides what a gap means.
func (d *Driver) GetMany(ctx context.Context, res *database.Resource, keys []string) ([]proto.Message, error) {
	if res == nil {
		return nil, fmt.Errorf("redis: GetMany needs a resource")
	}
	if len(keys) == 0 {
		return nil, nil
	}

	out := make([]proto.Message, len(keys))
	for start := 0; start < len(keys); start += batchSize {
		end := min(start+batchSize, len(keys))
		pipe := d.rdb.Pipeline()
		cmds := make([]*goredis.StringCmd, end-start)
		for i, key := range keys[start:end] {
			cmds[i] = pipe.Get(ctx, d.keys.record(res, key))
		}
		// A nil reply from any one command surfaces here as the pipeline's
		// error. That is a miss, not a failure, and the per-command results
		// below say which.
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
			return nil, fmt.Errorf("redis: cannot read %s: %w", res.Name, err)
		}
		for i, cmd := range cmds {
			body, err := cmd.Bytes()
			if errors.Is(err, goredis.Nil) {
				continue // a miss leaves nil in place
			}
			if err != nil {
				return nil, fmt.Errorf("redis: cannot read %s: %w", res.Name, err)
			}
			msg, derr := decode(res, body)
			if derr != nil {
				return nil, derr
			}
			out[start+i] = msg
		}
	}
	return out, nil
}

// CreateMany stores every message, reporting one result per message.
//
// It is a loop, not a pipeline, and that is deliberate: [Driver.Create] claims a
// primary key and every unique value with SET NX and rolls back what it claimed
// on a conflict. Pipelining that would mean deciding what to do about the third
// message conflicting after the first two were written, which is a transaction —
// and Redis does not have one across these keys.
//
// So this exists for the shape rather than the speed, and stops at the first
// failure with the results so far. Where uniqueness is not in play, the write
// path is a single SET NX and a loop is already close to a pipeline's cost.
func (d *Driver) CreateMany(ctx context.Context, res *database.Resource, msgs []proto.Message) ([]database.WriteResult, error) {
	out := make([]database.WriteResult, 0, len(msgs))
	for i, msg := range msgs {
		r, err := d.Create(ctx, res, msg)
		if err != nil {
			return out, fmt.Errorf("redis: CreateMany stopped at index %d: %w", i, err)
		}
		out = append(out, r)
	}
	return out, nil
}
