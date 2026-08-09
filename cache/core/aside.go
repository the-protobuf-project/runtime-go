package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
//
// # Cost of a hit and a miss
//
// A hit is one round trip. A miss is one round trip and then, for exactly one
// caller per id, the load. Both numbers are per caller and neither grows with
// how many callers arrive at once, which is the property that decides whether a
// hot key is survivable.
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

	// refreshing holds the ids with a background refresh already admitted, so a
	// hot key going stale spends one slot of the budget rather than all of them.
	//
	// Without it, a thousand readers finding one stale entry each asked for a
	// refresh: the first sixty-four were admitted, sixty-three of those went
	// straight to waiting on the same single-flight, and any other key going
	// stale in that moment found the budget gone.
	refreshing sync.Map
}

var _ cache.Aside = (*aside)(nil)

// GetOrLoad returns the cached value, loading it when there is none.
func (s *aside) GetOrLoad(ctx context.Context, id string, dest any, opts ...cache.Option) error {
	o := cache.NewOptions(s.def, opts...)

	got, err := s.read(ctx, id)
	switch {
	case err == nil:
		// A remembered absence. Without this, requests for something that does
		// not exist reach the loader forever — the cache does nothing for
		// exactly the traffic a scraper produces.
		if got.void {
			return fmt.Errorf("%w: %s", cache.ErrNotFound, id)
		}
		// The hit path: one round trip, which is every request but the first.
		if got.stale {
			// Refresh behind this reader, who gets the old value now. Declined
			// silently when the budget is full or another refresh of this id is
			// already running — the entry is still servable either way.
			s.background(id, o)
		}
		return decode(got.value, dest)
	case !errors.Is(err, ErrMiss):
		return err
	}

	body, _, err := s.flight.Do(ctx, id, func(ctx context.Context) ([]byte, error) {
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

// Invalidate drops the entry, and the remembered absence with it — they are one
// key, so something that did not exist a minute ago and does now becomes
// visible again in a single round trip.
func (s *aside) Invalidate(ctx context.Context, id string) error {
	if err := s.driver.Delete(ctx, s.keys.entry(id)); err != nil {
		return fmt.Errorf("cache: cannot invalidate %s: %w", id, err)
	}
	return nil
}

// read returns what the stored frame says, in one round trip.
func (s *aside) read(ctx context.Context, id string) (unpacked, error) {
	body, err := s.driver.Get(ctx, s.keys.entry(id))
	if err != nil {
		return unpacked{}, err
	}
	return unpack(body)
}

// background refreshes an entry behind the reader that noticed it was stale,
// through the same flight so a thousand stale reads cause one load.
//
// The claim comes first and is a map operation, not a lock: on a hot key every
// reader but one is turned away here, before touching the refresher's mutex.
func (s *aside) background(id string, o cache.Options) {
	if _, busy := s.refreshing.LoadOrStore(id, struct{}{}); busy {
		return
	}
	admitted := s.fresh.Go(func(ctx context.Context) {
		defer s.refreshing.Delete(id)
		_, _, _ = s.flight.Do(ctx, id, func(ctx context.Context) ([]byte, error) {
			return s.loadAndStore(ctx, id, o)
		})
	})
	if !admitted {
		s.refreshing.Delete(id)
	}
}
