package resp

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/cache/core"
)

var (
	_ core.Bulk   = primitives{}
	_ core.Fenced = primitives{}
)

// GetMany fetches many keys in one round trip.
//
// A pipeline rather than MGET, because MGET requires every key to live in one
// hash slot and a cluster client would reject the call. The driver splits a
// pipeline across nodes on its own, so this is the form that works everywhere
// and costs nothing extra where clustering is not in play.
func (p primitives) GetMany(ctx context.Context, keys []string) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	pipe := p.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(keys))
	for i, key := range keys {
		cmds[i] = pipe.Get(ctx, key)
	}
	// A nil reply from any one command surfaces here as the pipeline's error.
	// That is a miss, not a failure, and the per-command results below say which.
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	out := make([][]byte, len(keys))
	for i, cmd := range cmds {
		body, err := cmd.Bytes()
		if errors.Is(err, redis.Nil) {
			continue // a miss leaves nil in place
		}
		if err != nil {
			return nil, err
		}
		out[i] = body
	}
	return out, nil
}

// ExistsMany reports liveness for many keys in one round trip.
//
// EXISTS accepts several keys but answers with a count, which cannot say which
// ones were there — so this is a pipeline of single-key checks, and still one
// network round trip rather than one per key.
func (p primitives) ExistsMany(ctx context.Context, keys []string) ([]bool, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	pipe := p.client.Pipeline()
	cmds := make([]*redis.IntCmd, len(keys))
	for i, key := range keys {
		cmds[i] = pipe.Exists(ctx, key)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	out := make([]bool, len(keys))
	for i, cmd := range cmds {
		n, err := cmd.Result()
		if err != nil {
			return nil, err
		}
		out[i] = n > 0
	}
	return out, nil
}

// unlock deletes a key only if it still holds the expected value.
//
// The read and the delete have to be one step. Between a GET and a DEL the lease
// can lapse and another holder can take the key, and the DEL would then release
// a lock somebody else is working under — which produces a duplicate load at
// precisely the moment the lock existed to prevent one.
var unlock = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	end
	return 0
`)

// DeleteIf releases a lock this caller owns, and reports whether it did.
func (p primitives) DeleteIf(ctx context.Context, key string, value []byte) (bool, error) {
	n, err := unlock.Run(ctx, p.client, []string{key}, value).Int64()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
