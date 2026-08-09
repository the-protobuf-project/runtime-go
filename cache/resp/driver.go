package resp

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/cache/core"
)

// primitives is everything Redis contributes: eight required methods and three
// capabilities. No strategy, no keys, no encoding — those are core's, and they
// are the same here as on any other backend.
type primitives struct {
	client  redis.UniversalClient
	backend string
}

// A RESP server implements every optional capability core defines. The two in
// bulk.go — pipelined bulk reads and a scripted compare-and-delete — are the ones
// that make enumeration cheap and a cross-process lock safe.
var (
	_ core.Driver  = primitives{}
	_ core.Sets    = primitives{}
	_ core.Leases  = primitives{}
	_ core.Scanner = primitives{}
)

// Name is the server this driver is talking to, whichever RESP
// implementation that happens to be.
func (p primitives) Name() string { return p.backend }

// Get returns the stored bytes. A nil reply is a miss; anything else is a real
// failure and travels up as itself, so a dropped connection is never reported as
// an absent key.
func (p primitives) Get(ctx context.Context, key string) ([]byte, error) {
	body, err := p.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, core.ErrMiss
	}
	return body, err
}

// Set writes unconditionally. A zero ttl is no expiry, which is what the driver
// already means by it.
func (p primitives) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return p.client.Set(ctx, key, value, ttl).Err()
}

// Add writes only if the key is absent.
func (p primitives) Add(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	return p.client.SetNX(ctx, key, value, ttl).Result()
}

// Replace writes only if the key is present.
func (p primitives) Replace(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	return p.client.SetXX(ctx, key, value, ttl).Result()
}

// Delete removes keys.
func (p primitives) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return p.client.Del(ctx, keys...).Err()
}

// Exists reports whether a key is live without transferring its value.
func (p primitives) Exists(ctx context.Context, key string) (bool, error) {
	n, err := p.client.Exists(ctx, key).Result()
	return n > 0, err
}

// Touch extends a lease without rewriting the value.
//
// EXPIRE with a zero or negative argument deletes the key, so a request for no
// expiry is PERSIST instead — a Touch that silently destroyed what it was asked
// to keep alive would be a memorable outage.
func (p primitives) Touch(ctx context.Context, key string, ttl time.Duration) error {
	var (
		ok  bool
		err error
	)
	if ttl <= 0 {
		ok, err = p.client.Persist(ctx, key).Result()
	} else {
		ok, err = p.client.ExpireXX(ctx, key, ttl).Result()
	}
	if err != nil {
		return err
	}
	if !ok {
		return core.ErrMiss
	}
	return nil
}

// SetAdd files members in a set.
func (p primitives) SetAdd(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}
	return p.client.SAdd(ctx, key, toAny(members)...).Err()
}

// SetRemove drops members from a set.
func (p primitives) SetRemove(ctx context.Context, key string, members ...string) error {
	if len(members) == 0 {
		return nil
	}
	return p.client.SRem(ctx, key, toAny(members)...).Err()
}

// SetMembers reads a whole set.
func (p primitives) SetMembers(ctx context.Context, key string) ([]string, error) {
	return p.client.SMembers(ctx, key).Result()
}

// TTL reports the remaining lease.
//
// Redis answers with two negative sentinels rather than a duration: -1 for a key
// with no expiry, which the contract calls zero, and -2 for one that is gone,
// which is a miss.
func (p primitives) TTL(ctx context.Context, key string) (time.Duration, error) {
	ttl, err := p.client.TTL(ctx, key).Result()
	switch {
	case err != nil:
		return 0, err
	case ttl == -2:
		return 0, core.ErrMiss
	case ttl < 0:
		return 0, nil
	default:
		return ttl, nil
	}
}

// Scan walks the keyspace with a cursor, never KEYS, which blocks the server for
// as long as the whole keyspace takes to read.
func (p primitives) Scan(ctx context.Context, pattern string) ([]string, error) {
	var (
		out    []string
		cursor uint64
	)
	for {
		keys, next, err := p.client.Scan(ctx, cursor, pattern, 256).Result()
		if err != nil {
			return nil, err
		}
		out = append(out, keys...)
		if next == 0 {
			return out, nil
		}
		cursor = next
	}
}

// toAny adapts a string slice to the driver's variadic any.
func toAny(members []string) []any {
	out := make([]any, len(members))
	for i, m := range members {
		out[i] = m
	}
	return out
}
