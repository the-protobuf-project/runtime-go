package redis

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/redis/internal/codec"
	"github.com/the-protobuf-project/runtime-go/redis/internal/conn"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/the-protobuf-project/runtime-go/ulid"
)

// cacheHandler is ephemeral, TTL-bound storage.
type cacheHandler struct {
	conn *conn.Conn
	keys keys
	log  telemetry.Logger
}

var _ cache.Cache = (*cacheHandler)(nil)

func newCache(c *conn.Conn, prefix string, log telemetry.Logger, _ telemetry.Meter) cache.Cache {
	return &cacheHandler{conn: c, keys: newKeys(prefix, kindCache), log: log}
}

// notFound turns a missing key into the contract's sentinel, leaving every
// other failure — a dropped connection, a timeout, a WRONGTYPE reply — alone so
// a transport error is never misreported as a miss.
func cacheNotFound(id string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, goredis.Nil) {
		return fmt.Errorf("%w: %s", cache.ErrNotFound, id)
	}
	return err
}

// Create stores a value under id, generating one when id is empty.
func (c *cacheHandler) Create(ctx context.Context, id string, value any, opts ...cache.Option) (string, error) {
	o := cache.NewOptions(opts...)

	if id == "" {
		id = ulid.Generate().GetRandomCode()
		c.log.Debug(ctx, "generated an id", telemetry.Fields{"id": id})
	}

	body, err := codec.Encode(value)
	if err != nil {
		c.log.Error(ctx, "could not encode the value", err, telemetry.Fields{"id": id})
		return "", err
	}

	key := c.keys.entry(id)
	c.log.Debug(ctx, "writing cache entry", telemetry.Fields{
		"id": id, "key": key, "ttl": o.TTL.String(), "bytes": len(body),
	})

	// The entry and its index member go together, so a listing never names an
	// entry that was never written.
	pipe := c.conn.Redis().TxPipeline()
	pipe.Set(ctx, key, body, o.TTL)
	pipe.SAdd(ctx, c.keys.index(), id)
	if _, err := pipe.Exec(ctx); err != nil {
		c.log.Error(ctx, "could not write the cache entry", err, telemetry.Fields{"id": id, "key": key})
		return "", fmt.Errorf("redis: cannot create cache entry %s: %w", id, err)
	}

	c.log.Debug(ctx, "cache entry written", telemetry.Fields{"id": id})
	return id, nil
}

// Get decodes an entry into dest.
func (c *cacheHandler) Get(ctx context.Context, id string, dest any) error {
	key := c.keys.entry(id)
	c.log.Debug(ctx, "reading cache entry", telemetry.Fields{"id": id, "key": key})

	body, err := c.conn.Redis().Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			c.log.Debug(ctx, "cache miss", telemetry.Fields{"id": id})
		} else {
			c.log.Error(ctx, "could not read the cache entry", err, telemetry.Fields{"id": id})
		}
		return cacheNotFound(id, err)
	}

	if err := codec.Decode(body, dest); err != nil {
		c.log.Error(ctx, "stored entry does not decode into the destination", err,
			telemetry.Fields{"id": id, "dest": fmt.Sprintf("%T", dest)})
		return err
	}
	return nil
}

// Update replaces an entry, reporting a missing one rather than creating it.
func (c *cacheHandler) Update(ctx context.Context, id string, value any, opts ...cache.Option) error {
	o := cache.NewOptions(opts...)

	body, err := codec.Encode(value)
	if err != nil {
		c.log.Error(ctx, "could not encode the value", err, telemetry.Fields{"id": id})
		return err
	}

	key := c.keys.entry(id)
	c.log.Debug(ctx, "updating cache entry", telemetry.Fields{
		"id": id, "key": key, "ttl": o.TTL.String(),
	})

	// SET with XX writes only when the key is already there, so the existence
	// check and the write are one step — a separate GET first would let the
	// entry expire in between and recreate it here.
	ok, err := c.conn.Redis().SetXX(ctx, key, body, o.TTL).Result()
	if err != nil {
		c.log.Error(ctx, "could not update the cache entry", err, telemetry.Fields{"id": id})
		return fmt.Errorf("redis: cannot update cache entry %s: %w", id, err)
	}
	if !ok {
		c.log.Debug(ctx, "cache entry absent, nothing updated", telemetry.Fields{"id": id})
		return fmt.Errorf("%w: %s", cache.ErrNotFound, id)
	}

	c.log.Debug(ctx, "cache entry updated", telemetry.Fields{"id": id})
	return nil
}

// Delete removes an entry.
//
// Deleting one that is not there is not an error: the caller wanted it gone,
// and it may legitimately have expired a moment earlier.
func (c *cacheHandler) Delete(ctx context.Context, id string) error {
	c.log.Debug(ctx, "deleting cache entry", telemetry.Fields{"id": id})

	pipe := c.conn.Redis().TxPipeline()
	pipe.Del(ctx, c.keys.entry(id))
	pipe.SRem(ctx, c.keys.index(), id)
	if _, err := pipe.Exec(ctx); err != nil {
		c.log.Error(ctx, "could not delete the cache entry", err, telemetry.Fields{"id": id})
		return fmt.Errorf("redis: cannot delete cache entry %s: %w", id, err)
	}

	c.log.Debug(ctx, "cache entry deleted", telemetry.Fields{"id": id})
	return nil
}

// Keys returns the ids of every live entry, sweeping those that expired.
//
// Entries expire on their own but leave their index member behind, so without
// this the index grows without bound in a cache with short TTLs.
func (c *cacheHandler) Keys(ctx context.Context) ([]string, error) {
	ids, err := c.conn.Redis().SMembers(ctx, c.keys.index()).Result()
	if err != nil {
		c.log.Error(ctx, "could not read the cache index", err, nil)
		return nil, fmt.Errorf("redis: cannot read the cache index: %w", err)
	}

	live := make([]string, 0, len(ids))
	swept := 0
	for _, id := range ids {
		n, err := c.conn.Redis().Exists(ctx, c.keys.entry(id)).Result()
		if err != nil {
			c.log.Error(ctx, "could not check a cache entry", err, telemetry.Fields{"id": id})
			return nil, fmt.Errorf("redis: cannot check cache entry %s: %w", id, err)
		}
		if n == 0 {
			// Expired between the index read and now. A cleanup failure is not
			// worth failing the read over — the next call tries again.
			if remErr := c.conn.Redis().SRem(ctx, c.keys.index(), id).Err(); remErr != nil {
				c.log.Warn(ctx, "could not sweep a stale index member",
					telemetry.Fields{"id": id, "error": remErr.Error()})
			} else {
				swept++
			}
			continue
		}
		live = append(live, id)
	}

	if swept > 0 {
		c.log.Debug(ctx, "swept expired index members",
			telemetry.Fields{"swept": swept, "live": len(live)})
	}
	return live, nil
}

// List decodes every live entry into dest, which must point to a slice.
func (c *cacheHandler) List(ctx context.Context, dest any) error {
	ids, err := c.Keys(ctx)
	if err != nil {
		return err
	}

	slice, elem, err := sliceTarget(dest)
	if err != nil {
		c.log.Error(ctx, "list destination is not a slice pointer", err,
			telemetry.Fields{"dest": fmt.Sprintf("%T", dest)})
		return err
	}

	out := reflect.MakeSlice(slice.Type(), 0, len(ids))
	for _, id := range ids {
		item := reflect.New(elem)
		if gerr := c.Get(ctx, id, item.Interface()); gerr != nil {
			// Expired between the sweep and now; a listing is a snapshot, not a
			// transaction, so skip it rather than fail.
			if errors.Is(gerr, cache.ErrNotFound) {
				continue
			}
			return gerr
		}
		out = reflect.Append(out, item.Elem())
	}

	slice.Set(out)
	c.log.Debug(ctx, "listed cache entries", telemetry.Fields{"count": out.Len()})
	return nil
}

// TTL reports how much longer an entry lives.
//
// Redis answers with two negative sentinels rather than a duration: -1 for a key
// with no expiry and -2 for one that is gone. The first is reported as zero, which
// is what the contract means by "does not expire"; the second is a miss.
func (c *cacheHandler) TTL(ctx context.Context, id string) (time.Duration, error) {
	ttl, err := c.conn.Redis().TTL(ctx, c.keys.entry(id)).Result()
	if err != nil {
		c.log.Error(ctx, "could not read the entry TTL", err, telemetry.Fields{"id": id})
		return 0, fmt.Errorf("redis: cannot read the TTL of %s: %w", id, err)
	}

	switch {
	case ttl == -2:
		c.log.Debug(ctx, "cache miss on TTL", telemetry.Fields{"id": id})
		return 0, fmt.Errorf("%w: %s", cache.ErrNotFound, id)
	case ttl < 0:
		return 0, nil
	default:
		return ttl, nil
	}
}

// sliceTarget validates a List destination and returns the slice to fill and
// its element type.
func sliceTarget(dest any) (reflect.Value, reflect.Type, error) {
	if dest == nil {
		return reflect.Value{}, nil, fmt.Errorf("redis: list destination is nil")
	}
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return reflect.Value{}, nil, fmt.Errorf("redis: list destination must be a non-nil pointer, got %T", dest)
	}
	slice := v.Elem()
	if slice.Kind() != reflect.Slice {
		return reflect.Value{}, nil, fmt.Errorf("redis: list destination must point to a slice, got %T", dest)
	}
	return slice, slice.Type().Elem(), nil
}
