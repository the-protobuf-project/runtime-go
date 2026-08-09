package core

import (
	"context"
	"sync"
	"time"
)

// flight collapses concurrent work on the same key into one execution, in this
// process, with no network involved.
//
// This is the first line of defense against a stampede and by far the cheapest.
// A cross-process lock costs a round trip to take, another to release, and a way
// for everyone else to find out it was released; this costs a map lookup. With
// it, a thousand concurrent misses in one process become one load — so a fleet
// of ten does at most ten, bounded by processes rather than by requests.
type flight struct {
	mu      sync.Mutex
	calls   map[string]*call
	timeout time.Duration
}

// call is one execution and the goroutines waiting on it.
type call struct {
	done chan struct{}
	body []byte
	err  error
}

func newFlight(timeout time.Duration) *flight {
	return &flight{calls: make(map[string]*call), timeout: timeout}
}

// Do runs fn for key, or joins the run already under way, and reports whether
// this caller was the one that ran it.
//
// Nobody blocks past their own context, including the caller that started the
// work: the execution belongs to the flight, not to whoever arrived first, so a
// request that times out or is canceled leaves it running for everyone still
// waiting. That is the whole reason fn runs in a goroutine rather than inline.
func (f *flight) Do(ctx context.Context, key string, fn func(context.Context) ([]byte, error)) ([]byte, bool, error) {
	f.mu.Lock()
	c, joined := f.calls[key]
	if !joined {
		c = &call{done: make(chan struct{})}
		f.calls[key] = c
		go f.run(ctx, key, c, fn)
	}
	f.mu.Unlock()

	select {
	case <-c.done:
		return c.body, !joined, c.err
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

// run executes fn and publishes the result to everyone waiting.
func (f *flight) run(ctx context.Context, key string, c *call, fn func(context.Context) ([]byte, error)) {
	// Detached from the caller that happened to arrive first, and bounded by a
	// timeout of its own. Inheriting that caller's context would let one
	// canceled request fail a load that a hundred others are waiting on — a
	// failure mode that only appears under the load it makes worst.
	load, cancel := context.WithTimeout(context.WithoutCancel(ctx), f.timeout)
	defer cancel()

	// Removed from the map before the result is published, so a caller arriving
	// in between starts a fresh execution rather than joining a finished one.
	defer close(c.done)
	defer func() {
		f.mu.Lock()
		delete(f.calls, key)
		f.mu.Unlock()
	}()

	c.body, c.err = fn(load)
}
