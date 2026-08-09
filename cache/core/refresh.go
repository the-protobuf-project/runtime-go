package core

import (
	"context"
	"errors"
	"sync"
	"time"
)

// refresher runs background work under a fixed budget.
//
// It backs serving a stale entry while a fresh one is fetched, and its two rules
// are what keep that from becoming a liability. The budget is a count of
// goroutines, not a queue: when it is full, a refresh is declined rather than
// deferred, because the entry is still servable and the next reader will ask
// again. And every refresh is waited for at shutdown, so closing a database does
// not leave work running against a client someone is about to close.
type refresher struct {
	mu      sync.Mutex
	closed  bool
	wg      sync.WaitGroup
	slots   chan struct{}
	timeout time.Duration
}

func newRefresher(limit int, timeout time.Duration) *refresher {
	if limit < 1 {
		limit = 1
	}
	return &refresher{slots: make(chan struct{}, limit), timeout: timeout}
}

// Go runs fn in the background, reporting whether it was admitted.
//
// The context fn receives is not derived from any request: background work
// outlives the reader that noticed it was needed, and inheriting that reader's
// context would cancel the refresh the moment the response was written — leaving
// the entry stale, which would make the next reader trigger another refresh that
// is canceled the same way, forever.
func (r *refresher) Go(fn func(context.Context)) bool {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return false
	}
	select {
	case r.slots <- struct{}{}:
	default:
		r.mu.Unlock()
		return false // budget full: drop it, the entry is still servable
	}
	r.wg.Add(1)
	r.mu.Unlock()

	go func() {
		defer r.wg.Done()
		defer func() { <-r.slots }()

		ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
		defer cancel()
		fn(ctx)
	}()
	return true
}

// Drain stops admitting work and waits for what is running, up to a limit.
//
// The limit matters: a refresh blocked on an unreachable server would otherwise
// hold Close open for as long as its own timeout, and a program shutting down
// has somewhere better to be.
func (r *refresher) Drain(limit time.Duration) error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(limit):
		return errors.New("cache: background refreshes did not finish in time")
	}
}
