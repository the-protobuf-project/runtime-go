package cache

import (
	"context"
	"errors"
	"time"
)

// retryCache retries operations that failed for a reason a retry could fix.
type retryCache struct {
	next     Document
	attempts int
	backoff  time.Duration
}

// WithRetry retries failed operations with exponential backoff, up to attempts
// total tries (so attempts=3 means one try and two retries). A non-positive
// attempts count disables retrying and returns next unchanged.
//
// Reads and deletes are retried; writes are not — Create and Update are not
// idempotent here (Create can mint an id, Update overwrites), and replaying a
// half-applied write can duplicate an entry rather than repair one. Retrying a
// [ErrNotFound] is likewise pointless: the answer will not change.
//
// Backoff respects the context: a canceled or expired ctx stops the retry loop
// immediately rather than sleeping out the remaining attempts.
func WithRetry(next Document, attempts int, backoff time.Duration) Document {
	if attempts <= 1 {
		return next
	}
	return &retryCache{next: next, attempts: attempts, backoff: backoff}
}

// WithRetryMiddleware is [WithRetry] as a [Middleware], for use with [Chain].
func WithRetryMiddleware(attempts int, backoff time.Duration) Middleware {
	return func(c Document) Document { return WithRetry(c, attempts, backoff) }
}

// retry runs op until it succeeds, the context ends, or the attempts run out.
// It returns the last error seen.
func (r *retryCache) retry(ctx context.Context, op func() error) error {
	var err error
	wait := r.backoff

	for i := range r.attempts {
		if err = op(); err == nil {
			return nil
		}
		// A missing key is a settled answer; retrying only wastes the caller's
		// deadline.
		if errors.Is(err, ErrNotFound) {
			return err
		}
		if i == r.attempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(wait):
			wait *= 2
		}
	}
	return err
}

// Create is not retried; see [WithRetry].
func (r *retryCache) Create(ctx context.Context, id string, value any, opts ...Option) (string, error) {
	return r.next.Create(ctx, id, value, opts...)
}

// Update is not retried; see [WithRetry].
func (r *retryCache) Update(ctx context.Context, id string, value any, opts ...Option) error {
	return r.next.Update(ctx, id, value, opts...)
}

func (r *retryCache) Get(ctx context.Context, id string, dest any) error {
	return r.retry(ctx, func() error { return r.next.Get(ctx, id, dest) })
}

func (r *retryCache) Delete(ctx context.Context, id string) error {
	return r.retry(ctx, func() error { return r.next.Delete(ctx, id) })
}

func (r *retryCache) Keys(ctx context.Context) ([]string, error) {
	var keys []string
	err := r.retry(ctx, func() error {
		var err error
		keys, err = r.next.Keys(ctx)
		return err
	})
	return keys, err
}

func (r *retryCache) List(ctx context.Context, dest any) error {
	return r.retry(ctx, func() error { return r.next.List(ctx, dest) })
}

func (r *retryCache) TTL(ctx context.Context, id string) (time.Duration, error) {
	var ttl time.Duration
	err := r.retry(ctx, func() error {
		var err error
		ttl, err = r.next.TTL(ctx, id)
		return err
	})
	return ttl, err
}
