package cached

import (
	"context"
	"errors"
	"fmt"

	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/database"
)

// FromAside adapts a cache database into a [Cache].
//
// This is the wiring for the runtime-go cache module, and it is the whole of it:
//
//	c, _ := redis.NewClient(ctx, redis.Config{Address: "localhost:6379"})
//	cdb, _ := rediscache.New(c, cache.Config{DefaultTTL: 5 * time.Minute}).
//	    SetDatabase(ctx, "orders")
//
//	db = cached.Wrap(db, cached.FromAside(cdb, cache.TTL(time.Minute)))
//
// # A TTL is not optional here
//
// Every write through the decorator invalidates what it wrote, so in the ordinary
// case entries do not go stale. The TTL is for the cases outside that: a write
// that reaches the store and fails to invalidate, a migration, a
// [database.Migrator.DropSchema], another process writing to the same database
// without this decorator in front of it. Without one, a wrong entry is wrong
// until something happens to overwrite it, and nothing is guaranteed to.
func FromAside(db *cache.DB, opts ...cache.Option) Cache {
	return asideCache{db: db, opts: opts}
}

// asideCache builds one read-through view per resource over a cache database.
type asideCache struct {
	db   *cache.DB
	opts []cache.Option
}

var _ Cache = asideCache{}

// Aside binds a loader to a read-through view.
//
// The two error translations here are what make the decorator's contract hold.
// A [database.ErrNotFound] from the loader becomes the cache's own sentinel, which
// is what tells it to remember the absence; on the way back out it becomes
// store's again, so the adapter above never learns which cache it is talking to.
func (a asideCache) Aside(load func(ctx context.Context, key string) ([]byte, error)) Aside {
	inner := a.db.Aside(func(ctx context.Context, id string) (any, error) {
		body, err := load(ctx, id)
		if err != nil {
			if errors.Is(err, database.ErrNotFound) {
				// Translated so the cache remembers the absence. Without this it
				// sees an ordinary failure, caches nothing, and every request
				// for a record that does not exist reaches the database.
				return nil, fmt.Errorf("%w: %s", cache.ErrNotFound, id)
			}
			return nil, err
		}
		return body, nil
	})
	return asideView{inner: inner, opts: a.opts}
}

// asideView is one resource's read-through view over a cache.Aside.
type asideView struct {
	inner cache.Aside
	opts  []cache.Option
}

var _ Aside = asideView{}

// GetOrLoad fills dest with the record's bytes.
//
// The value handed to the cache is a []byte, which its JSON codec stores as
// base64 and gives back byte for byte. That costs about a third more space than
// the wire format and is the only encoding that survives: bytes that are not
// valid UTF-8 and integers past 2^53 both come back wrong through any other
// JSON shape, and both come back silently wrong.
func (v asideView) GetOrLoad(ctx context.Context, key string, dest *[]byte) error {
	if dest == nil {
		return fmt.Errorf("cached: GetOrLoad needs a destination")
	}
	err := v.inner.GetOrLoad(ctx, key, dest, v.opts...)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, cache.ErrNotFound):
		return fmt.Errorf("%w: %s", database.ErrNotFound, key)
	case errors.Is(err, cache.ErrOverloaded):
		return fmt.Errorf("%w: %s", ErrOverloaded, key)
	default:
		return err
	}
}

// Invalidate drops the entries for keys.
func (v asideView) Invalidate(ctx context.Context, keys ...string) error {
	var errs []error
	for _, key := range keys {
		if err := v.inner.Invalidate(ctx, key); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
