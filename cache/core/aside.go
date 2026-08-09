package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
)

// aside is read-through caching over a loader.
//
// Three mechanisms, in the order they matter:
//
//  1. Serving stale. Past its freshness deadline but inside its stale window, an
//     entry is returned immediately and refreshed in the background. A reader
//     arriving the microsecond after a hot key expires waits for nothing, which
//     is the only way a cache actually absorbs a spike rather than forwarding
//     it. Off unless a stale window is asked for.
//  2. In-process collapsing. Concurrent loads of one id share one execution, at
//     the cost of a map lookup and no round trips at all.
//  3. Cross-process collapsing, when the backend can release a lock safely.
//     Best-effort by construction: losing the race costs one extra read, never a
//     wait, so a lock that is slow, stuck or unavailable can never make a
//     request slower than not having one.
//
// Correctness never depends on 2 or 3. They bound how many times the loader
// runs; nothing above notices if they do not fire.
type aside struct {
	driver Driver
	fenced Fenced // nil when the backend cannot release a lock safely
	keys   Keyspace
	load   cache.Loader
	def    cache.Options
	flight *flight
	fresh  *refresher
	empty  time.Duration // how long an absence is remembered
	lease  time.Duration // how long the cross-process lock lives

	// require refuses a load whose write would have no expiry.
	require bool
}

var _ cache.Aside = (*aside)(nil)

// GetOrLoad returns the cached value, loading it when there is none.
func (s *aside) GetOrLoad(ctx context.Context, id string, dest any, opts ...cache.Option) error {
	o := cache.NewOptions(s.def, opts...)

	body, stale, err := s.read(ctx, id)
	switch {
	case err == nil:
		// The hit path: one round trip, which is every request but the first.
		if stale {
			// Refresh behind this reader, who gets the old value now. Declined
			// silently when the background budget is full — the entry is still
			// servable and the next reader will ask again.
			s.background(id, o)
		}
		return decode(body, dest)
	case !errors.Is(err, ErrMiss):
		return err
	}

	// An absence remembered from an earlier load. Without this, requests for
	// something that does not exist reach the loader forever — the cache does
	// nothing for exactly the traffic a scraper produces.
	void, err := s.voided(ctx, id)
	if err != nil {
		return err
	}
	if void {
		return fmt.Errorf("%w: %s", cache.ErrNotFound, id)
	}

	body, _, err = s.flight.Do(ctx, id, func(ctx context.Context) ([]byte, error) {
		return s.loadAndStore(ctx, id, o)
	})
	if err != nil {
		return err
	}
	return decode(body, dest)
}

// Refresh loads and overwrites, whether or not an entry was there.
//
// For a value you know just changed, call this rather than Invalidate: dropping
// the entry and waiting for the next reader to fault it in leaves a window where
// every reader misses at once, which is the stampede everything above exists to
// avoid and there is no reason to walk into it deliberately.
//
// Concurrent Refreshes of one id collapse, and one arriving while a load is
// already running joins it rather than starting a second — that load is already
// fetching what this call wanted.
func (s *aside) Refresh(ctx context.Context, id string, opts ...cache.Option) error {
	o := cache.NewOptions(s.def, opts...)
	_, _, err := s.flight.Do(ctx, id, func(ctx context.Context) ([]byte, error) {
		return s.loadAndStore(ctx, id, o)
	})
	return err
}

// Invalidate drops the entry and any remembered absence.
func (s *aside) Invalidate(ctx context.Context, id string) error {
	if err := s.driver.Delete(ctx, s.keys.entry(id), s.keys.void(id)); err != nil {
		return fmt.Errorf("cache: cannot invalidate %s: %w", id, err)
	}
	return nil
}

// read returns the stored value and whether it is past its freshness deadline.
func (s *aside) read(ctx context.Context, id string) ([]byte, bool, error) {
	body, err := s.driver.Get(ctx, s.keys.entry(id))
	if err != nil {
		return nil, false, err
	}
	value, stale, err := unpack(body)
	if err != nil {
		return nil, false, err
	}
	return value, stale, nil
}

// voided reports whether this id is known not to exist.
func (s *aside) voided(ctx context.Context, id string) (bool, error) {
	if _, err := s.driver.Get(ctx, s.keys.void(id)); err != nil {
		if errors.Is(err, ErrMiss) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// background refreshes an entry behind the reader that noticed it was stale,
// through the same flight so a thousand stale reads cause one load.
func (s *aside) background(id string, o cache.Options) {
	s.fresh.Go(func(ctx context.Context) {
		_, _, _ = s.flight.Do(ctx, id, func(ctx context.Context) ([]byte, error) {
			return s.loadAndStore(ctx, id, o)
		})
	})
}
