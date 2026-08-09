package cache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// fakeCache counts calls and returns whatever the test scripted.
type fakeCache struct {
	calls atomic.Int64
	errs  []error // consumed one per call; nil once exhausted
}

func (f *fakeCache) next() error {
	n := f.calls.Add(1)
	if int(n) <= len(f.errs) {
		return f.errs[n-1]
	}
	return nil
}

func (f *fakeCache) Create(_ context.Context, _ any, opts ...Option) (string, error) {
	o := NewOptions(Options{}, opts...)
	if o.ID == "" {
		return "generated", f.next()
	}
	return o.ID, f.next()
}
func (f *fakeCache) Get(context.Context, string, any) error               { return f.next() }
func (f *fakeCache) Update(context.Context, string, any, ...Option) error { return f.next() }
func (f *fakeCache) Delete(context.Context, string) error                 { return f.next() }
func (f *fakeCache) Keys(context.Context) ([]string, error)               { return nil, f.next() }
func (f *fakeCache) List(context.Context, any) error                      { return f.next() }
func (f *fakeCache) TTL(context.Context, string) (time.Duration, error)   { return 0, f.next() }

func TestWithRetryRetriesUntilSuccess(t *testing.T) {
	boom := errors.New("transient")
	f := &fakeCache{errs: []error{boom, boom}} // third call succeeds
	c := WithRetry(f, 3, time.Millisecond)

	if err := c.Get(t.Context(), "k", &struct{}{}); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := f.calls.Load(); got != 3 {
		t.Errorf("made %d calls, want 3", got)
	}
}

func TestWithRetryGivesUpAfterAttempts(t *testing.T) {
	boom := errors.New("persistent")
	f := &fakeCache{errs: []error{boom, boom, boom, boom}}
	c := WithRetry(f, 3, time.Millisecond)

	if err := c.Get(t.Context(), "k", &struct{}{}); !errors.Is(err, boom) {
		t.Fatalf("Get error = %v, want %v", err, boom)
	}
	if got := f.calls.Load(); got != 3 {
		t.Errorf("made %d calls, want exactly 3", got)
	}
}

// A missing key is a settled answer; retrying only burns the caller's deadline.
func TestWithRetryDoesNotRetryNotFound(t *testing.T) {
	f := &fakeCache{errs: []error{ErrNotFound, ErrNotFound, ErrNotFound}}
	c := WithRetry(f, 3, time.Millisecond)

	if err := c.Get(t.Context(), "k", &struct{}{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get error = %v, want ErrNotFound", err)
	}
	if got := f.calls.Load(); got != 1 {
		t.Errorf("made %d calls, want 1 — ErrNotFound must not be retried", got)
	}
}

// Create and Update are not idempotent here, so replaying them can duplicate a
// write rather than repair one.
func TestWithRetryDoesNotRetryWrites(t *testing.T) {
	boom := errors.New("transient")

	t.Run("create", func(t *testing.T) {
		f := &fakeCache{errs: []error{boom, boom, boom}}
		c := WithRetry(f, 3, time.Millisecond)
		if _, err := c.Create(t.Context(), nil); !errors.Is(err, boom) {
			t.Fatalf("Create error = %v", err)
		}
		if got := f.calls.Load(); got != 1 {
			t.Errorf("Create made %d calls, want 1", got)
		}
	})

	t.Run("update", func(t *testing.T) {
		f := &fakeCache{errs: []error{boom, boom, boom}}
		c := WithRetry(f, 3, time.Millisecond)
		if err := c.Update(t.Context(), "k", nil); !errors.Is(err, boom) {
			t.Fatalf("Update error = %v", err)
		}
		if got := f.calls.Load(); got != 1 {
			t.Errorf("Update made %d calls, want 1", got)
		}
	})
}

// Backoff must watch the context, not sleep through a cancellation.
func TestWithRetryStopsOnContextCancellation(t *testing.T) {
	boom := errors.New("transient")
	f := &fakeCache{errs: []error{boom, boom, boom, boom, boom}}
	c := WithRetry(f, 5, time.Hour) // would sleep forever if ctx were ignored

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	done := make(chan error, 1)
	go func() {
		done <- c.Get(ctx, "k", &struct{}{})
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want it to join context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retry slept through a canceled context")
	}
}

// A non-positive attempts count means "no retrying"; returning the wrapper
// anyway would add an allocation and an indirection for nothing.
func TestWithRetryDisabledReturnsInner(t *testing.T) {
	f := &fakeCache{}
	if got := WithRetry(f, 1, time.Second); got != Document(f) {
		t.Error("WithRetry(attempts=1) wrapped the cache; want the original back")
	}
	if got := WithRetry(f, 0, time.Second); got != Document(f) {
		t.Error("WithRetry(attempts=0) wrapped the cache; want the original back")
	}
}

// A nil meter must be tolerated — it is the natural thing to pass when
// telemetry is not configured, and panicking there would be hostile.
func TestWithTelemetryToleratesNilMeter(t *testing.T) {
	f := &fakeCache{}
	c := WithTelemetry(f, nil)

	if err := c.Get(t.Context(), "k", &struct{}{}); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := f.calls.Load(); got != 1 {
		t.Errorf("made %d calls, want 1", got)
	}
}

// Telemetry must be transparent: it observes, it does not alter results.
func TestWithTelemetryPassesResultsThrough(t *testing.T) {
	boom := errors.New("backend down")
	f := &fakeCache{errs: []error{boom}}
	c := WithTelemetry(f, nil)

	if err := c.Get(t.Context(), "k", &struct{}{}); !errors.Is(err, boom) {
		t.Errorf("Get error = %v, want %v", err, boom)
	}
}

func TestChainAppliesMiddlewareInOrder(t *testing.T) {
	f := &fakeCache{errs: []error{errors.New("transient")}}
	c := Chain(f, WithRetryMiddleware(2, time.Millisecond), WithTelemetryMiddleware(nil))

	if err := c.Get(t.Context(), "k", &struct{}{}); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := f.calls.Load(); got != 2 {
		t.Errorf("made %d calls, want 2 — the retry middleware was not applied", got)
	}
}
