package core

import (
	"context"
	"errors"
	"sync"
)

// batchSize is how many keys go into one bulk round trip. Large enough that the
// per-trip overhead disappears, small enough that one slow reply does not hold a
// megabyte of results hostage.
const batchSize = 256

// defaultConcurrency bounds the fan-out when a driver cannot do bulk. It is a
// budget, not a target: the point is that a listing of ten thousand entries
// costs ten thousand round trips spread over sixteen connections rather than
// ten thousand in a row, without opening ten thousand at once.
const defaultConcurrency = 16

// gather runs fn over items with bounded concurrency, preserving order.
//
// The first failure cancels the rest. A caller that asked for a listing has no
// use for a partial one, and continuing to spend round trips on a result nobody
// will read is worse than stopping.
func gather[T any](ctx context.Context, limit int, items []string, fn func(context.Context, string) (T, error)) ([]T, error) {
	out := make([]T, len(items))
	if len(items) == 0 {
		return out, nil
	}
	if limit < 1 {
		limit = 1
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg    sync.WaitGroup
		once  sync.Once
		first error
	)
	slots := make(chan struct{}, limit)

	for i, item := range items {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()

			value, err := fn(ctx, item)
			if err != nil {
				once.Do(func() { first = err; cancel() })
				return
			}
			out[i] = value // distinct index per goroutine; no sharing
		}()
	}
	wg.Wait()

	if first != nil {
		return nil, first
	}
	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return nil, err
	}
	return out, nil
}

// existsAll reports liveness for many keys, in one round trip per batch where
// the driver can, and in parallel where it cannot.
func existsAll(ctx context.Context, driver Driver, bulk Bulk, limit int, keys []string) ([]bool, error) {
	if bulk == nil {
		return gather(ctx, limit, keys, driver.Exists)
	}
	out := make([]bool, 0, len(keys))
	for start := 0; start < len(keys); start += batchSize {
		end := min(start+batchSize, len(keys))
		found, err := bulk.ExistsMany(ctx, keys[start:end])
		if err != nil {
			return nil, err
		}
		out = append(out, found...)
	}
	return out, nil
}

// getAll fetches many keys, reporting a miss as a nil entry rather than an
// error: a read racing an expiry is ordinary, and the caller decides what a gap
// means.
func getAll(ctx context.Context, driver Driver, bulk Bulk, limit int, keys []string) ([][]byte, error) {
	if bulk == nil {
		return gather(ctx, limit, keys, func(ctx context.Context, key string) ([]byte, error) {
			body, err := driver.Get(ctx, key)
			if errors.Is(err, ErrMiss) {
				return nil, nil
			}
			return body, err
		})
	}
	out := make([][]byte, 0, len(keys))
	for start := 0; start < len(keys); start += batchSize {
		end := min(start+batchSize, len(keys))
		bodies, err := bulk.GetMany(ctx, keys[start:end])
		if err != nil {
			return nil, err
		}
		out = append(out, bodies...)
	}
	return out, nil
}
