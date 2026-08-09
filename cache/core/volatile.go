package core

import (
	"context"
	"fmt"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
)

// volatile is TTL-first storage with nothing tracking what it holds.
//
// Every method here is one driver call. That is the strategy: no index write to
// pair with the value, no sweep on read, and expiry that costs nothing because
// nothing has to be told about it.
type volatile struct {
	driver  Driver
	leases  Leases  // nil when the protocol reports no remaining TTL
	scanner Scanner // nil when the keyspace cannot be walked
	keys    Keyspace
	def     cache.Options
}

var _ cache.Volatile = (*volatile)(nil)

// Set writes a value under a key the caller named.
func (s *volatile) Set(ctx context.Context, key string, value any, opts ...cache.Option) error {
	o := cache.NewOptions(s.def, opts...)
	body, err := encode(value)
	if err != nil {
		return err
	}
	if serr := s.driver.Set(ctx, s.keys.raw(key), body, o.TTL); serr != nil {
		return fmt.Errorf("cache: cannot store %s: %w", key, serr)
	}
	return nil
}

// Get decodes an entry into dest.
func (s *volatile) Get(ctx context.Context, key string, dest any) error {
	body, err := s.driver.Get(ctx, s.keys.raw(key))
	if err != nil {
		return notFound(key, err)
	}
	return decode(body, dest)
}

// Delete removes an entry.
func (s *volatile) Delete(ctx context.Context, key string) error {
	if err := s.driver.Delete(ctx, s.keys.raw(key)); err != nil {
		return fmt.Errorf("cache: cannot delete %s: %w", key, err)
	}
	return nil
}

// Touch extends a lease without resending the value, which is what a sliding
// session window needs and what re-Setting the value to get it would make
// needlessly expensive on every request.
func (s *volatile) Touch(ctx context.Context, key string, ttl time.Duration) error {
	if err := s.driver.Touch(ctx, s.keys.raw(key), ttl); err != nil {
		return notFound(key, err)
	}
	return nil
}

// TTL reports how much longer an entry lives.
func (s *volatile) TTL(ctx context.Context, key string) (time.Duration, error) {
	if s.leases == nil {
		return 0, unsupported(s.driver.Name(), "report a remaining TTL", "the protocol never returns one")
	}
	ttl, err := s.leases.TTL(ctx, s.keys.raw(key))
	if err != nil {
		return 0, notFound(key, err)
	}
	return ttl, nil
}

// Scan returns the keys matching a pattern, qualified with this strategy's
// segment so it cannot wander into another one's keys.
//
// Still a snapshot of nothing in particular: keys may expire mid-walk, and a
// cursor can return the same key twice. It is for operators poking at a cache,
// not for application logic.
func (s *volatile) Scan(ctx context.Context, pattern string) ([]string, error) {
	if s.scanner == nil {
		return nil, unsupported(s.driver.Name(), "scan keys", "no cursor in the protocol")
	}
	keys, err := s.scanner.Scan(ctx, s.keys.pattern(pattern))
	if err != nil {
		return nil, fmt.Errorf("cache: cannot scan %s: %w", pattern, err)
	}
	return keys, nil
}
