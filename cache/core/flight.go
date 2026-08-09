package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
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

	// budget caps how many distinct loads may run at once. Joining one already
	// running costs nothing and is never refused; only starting a new one is.
	//
	// The bound exists because the map and its goroutines grow with distinct
	// keys, not with callers. A cold start or a client walking ids it has never
	// asked for produces one goroutine per key and nothing was stopping it —
	// which is not a slow cache but a dead process.
	budget chan struct{}
}

// call is one execution and the goroutines waiting on it.
type call struct {
	done chan struct{}
	body []byte
	err  error
}

func newFlight(timeout time.Duration, budget int) *flight {
	if budget < 1 {
		budget = 1
	}
	return &flight{
		calls:   make(map[string]*call),
		timeout: timeout,
		budget:  make(chan struct{}, budget),
	}
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
		// Claim a slot before publishing the call, so a refusal leaves nothing
		// behind for another caller to join and wait on forever.
		select {
		case f.budget <- struct{}{}:
		default:
			f.mu.Unlock()
			return nil, false, fmt.Errorf("%w: %d loads already running", cache.ErrOverloaded, cap(f.budget))
		}
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
	// The slot goes back last, once nothing can join this call any more.
	defer func() { <-f.budget }()
	defer close(c.done)
	defer func() {
		f.mu.Lock()
		delete(f.calls, key)
		f.mu.Unlock()
	}()

	c.body, c.err = fn(load)
}
