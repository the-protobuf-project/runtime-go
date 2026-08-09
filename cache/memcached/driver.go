package memcached

import (
	"context"
	"errors"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/the-protobuf-project/runtime-go/cache/core"
)

// primitives is everything memcached contributes: the eight required methods,
// and one optional capability.
//
// The absent ones are the interesting part. There is no SetAdd here because
// memcached has no sets, no TTL because the protocol never reports a remaining
// lease, no Scan because there is no cursor, and no DeleteIf because there is no
// conditional delete to fence a lock with. core asserts for those interfaces at
// construction, does not find them, and every strategy that needs one reports
// cache.ErrUnsupported — with no code in this package saying so.
//
// # Contexts
//
// The driver has none. Every method below drops the context it is handed,
// because the client underneath takes no deadline per call — only
// [Config.Timeout], applied to all of them. That is a real limitation: a request
// that gives up cannot pull its cache read back with it, and a slow server is
// bounded by that one timeout rather than by the caller's deadline.
type primitives struct {
	client *memcache.Client
}

func (p primitives) Name() string { return "memcached" }

// Get returns the stored bytes.
func (p primitives) Get(_ context.Context, key string) ([]byte, error) {
	item, err := p.client.Get(key)
	if errors.Is(err, memcache.ErrCacheMiss) {
		return nil, core.ErrMiss
	}
	if err != nil {
		return nil, err
	}
	return item.Value, nil
}

// Set writes unconditionally.
func (p primitives) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	return p.client.Set(&memcache.Item{Key: key, Value: value, Expiration: toExpiry(ttl)})
}

// Add writes only if the key is absent — memcached's add, which is what would
// make a cross-process lock possible if there were a way to release one safely.
func (p primitives) Add(_ context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	err := p.client.Add(&memcache.Item{Key: key, Value: value, Expiration: toExpiry(ttl)})
	if errors.Is(err, memcache.ErrNotStored) {
		return false, nil
	}
	return err == nil, err
}

// Replace writes only if the key is present.
func (p primitives) Replace(_ context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	err := p.client.Replace(&memcache.Item{Key: key, Value: value, Expiration: toExpiry(ttl)})
	if errors.Is(err, memcache.ErrNotStored) {
		return false, nil
	}
	return err == nil, err
}

// Delete removes keys.
//
// The protocol has no multi-key delete, so this is one round trip per key rather
// than the single command a RESP server manages. A miss is success: the caller
// wanted the key gone, and it may have expired a moment earlier.
func (p primitives) Delete(_ context.Context, keys ...string) error {
	for _, key := range keys {
		if err := p.client.Delete(key); err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
			return err
		}
	}
	return nil
}

// Exists reports whether a key is live.
//
// memcached has no existence check, so this is a get whose value is thrown away
// — the whole payload crosses the network to answer a yes-or-no question. It is
// why a sweep on this backend is nothing to do casually, and why [primitives.ExistsMany]
// matters more here than it does on a server with a cheap EXISTS.
func (p primitives) Exists(_ context.Context, key string) (bool, error) {
	_, err := p.client.Get(key)
	if errors.Is(err, memcache.ErrCacheMiss) {
		return false, nil
	}
	return err == nil, err
}

// Touch extends a lease without resending the value.
//
// memcached has this natively, which is the one place it fits a strategy better
// than a RESP server does: no conditional flag to remember, and no way to
// resurrect a key that has already gone.
func (p primitives) Touch(_ context.Context, key string, ttl time.Duration) error {
	err := p.client.Touch(key, toExpiry(ttl))
	if errors.Is(err, memcache.ErrCacheMiss) {
		return core.ErrMiss
	}
	return err
}
