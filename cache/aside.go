package cache

import "context"

// Loader produces the value for an id when the cache does not have it. Whatever
// it returns is what gets encoded.
//
// A Loader returning an error wrapping [ErrNotFound] means the thing genuinely
// does not exist, which is worth caching too — see [Aside.GetOrLoad].
type Loader func(ctx context.Context, id string) (any, error)

// Aside is read-through caching over a [Loader].
//
// This is the pattern every caller writes by hand around a cache — check, miss,
// load, store, return — and hand-written versions share three bugs. They
// stampede: a thousand concurrent misses on a hot key become a thousand
// identical loads, at the worst possible moment. They cache the value but not
// its absence, so a request for something that does not exist reloads forever.
// And they block every reader the instant a hot key expires, which is the one
// moment the thing behind the cache can least afford them.
//
// [Stale] answers the third. Given a window, an expired entry is served
// immediately and refreshed behind the reader, so after the first load nobody
// waits again.
type Aside interface {
	// GetOrLoad decodes the cached entry into dest, calling the loader on a miss
	// and storing what it returns.
	//
	// Concurrent loads of one id collapse into one execution, and no caller
	// blocks past its own context — a request that gives up leaves the load
	// running for the others still waiting on it. A loader reporting
	// [ErrNotFound] has that absence remembered for a while, so a stream of
	// requests for something that does not exist stops reaching the loader.
	//
	// With a [Stale] window, an entry past its TTL is returned as it is and
	// refreshed in the background; without one, readers wait for the load.
	GetOrLoad(ctx context.Context, id string, dest any, opts ...Option) error

	// Refresh runs the loader and overwrites the entry whether or not one was
	// there. For a value you know just changed, this is what to call — deleting
	// it and waiting for the next reader to fault it in leaves a window where
	// every reader misses at once.
	Refresh(ctx context.Context, id string, opts ...Option) error

	// Invalidate drops the entry, and any remembered absence with it: something
	// that did not exist a minute ago and does now would otherwise stay
	// invisible until that absence expired on its own.
	Invalidate(ctx context.Context, id string) error
}
