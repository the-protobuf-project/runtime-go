package database

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// fakeStore counts calls and returns whatever the test scripted.
type fakeStore struct {
	calls atomic.Int64
	errs  []error // consumed one per call; nil once exhausted
}

func (f *fakeStore) next() error {
	n := f.calls.Add(1)
	if int(n) <= len(f.errs) {
		return f.errs[n-1]
	}
	return nil
}

func (f *fakeStore) Create(context.Context, Document) (*Document, error) {
	return &Document{}, f.next()
}
func (f *fakeStore) Get(context.Context, string) (Document, error) {
	return Document{}, f.next()
}
func (f *fakeStore) Update(context.Context, string, Document) error { return f.next() }
func (f *fakeStore) Delete(context.Context, string) error           { return f.next() }
func (f *fakeStore) List(context.Context, Query) ([]Document, error) {
	return nil, f.next()
}

func TestWithRetryRetriesUntilSuccess(t *testing.T) {
	boom := errors.New("transient")
	f := &fakeStore{errs: []error{boom, boom}} // third call succeeds
	s := WithRetry(f, 3, time.Millisecond)

	if _, err := s.Get(t.Context(), "k"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := f.calls.Load(); got != 3 {
		t.Errorf("made %d calls, want 3", got)
	}
}

// Both are settled answers; retrying only burns the caller's deadline.
func TestWithRetryDoesNotRetrySettledErrors(t *testing.T) {
	for name, sentinel := range map[string]error{
		"not found": ErrNotFound,
		"duplicate": ErrDuplicate,
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeStore{errs: []error{sentinel, sentinel, sentinel}}
			s := WithRetry(f, 3, time.Millisecond)

			if _, err := s.Get(t.Context(), "k"); !errors.Is(err, sentinel) {
				t.Fatalf("Get error = %v, want %v", err, sentinel)
			}
			if got := f.calls.Load(); got != 1 {
				t.Errorf("made %d calls, want 1", got)
			}
		})
	}
}

// Replaying a half-applied write can store a second copy rather than repair
// the first.
func TestWithRetryDoesNotRetryWrites(t *testing.T) {
	boom := errors.New("transient")

	t.Run("create", func(t *testing.T) {
		f := &fakeStore{errs: []error{boom, boom, boom}}
		s := WithRetry(f, 3, time.Millisecond)
		if _, err := s.Create(t.Context(), Document{}); !errors.Is(err, boom) {
			t.Fatalf("Create error = %v", err)
		}
		if got := f.calls.Load(); got != 1 {
			t.Errorf("Create made %d calls, want 1", got)
		}
	})

	t.Run("update", func(t *testing.T) {
		f := &fakeStore{errs: []error{boom, boom, boom}}
		s := WithRetry(f, 3, time.Millisecond)
		if err := s.Update(t.Context(), "k", Document{}); !errors.Is(err, boom) {
			t.Fatalf("Update error = %v", err)
		}
		if got := f.calls.Load(); got != 1 {
			t.Errorf("Update made %d calls, want 1", got)
		}
	})
}

func TestWithRetryStopsOnContextCancellation(t *testing.T) {
	boom := errors.New("transient")
	f := &fakeStore{errs: []error{boom, boom, boom, boom, boom}}
	s := WithRetry(f, 5, time.Hour) // would sleep forever if ctx were ignored

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	done := make(chan error, 1)
	go func() {
		_, err := s.Get(ctx, "k")
		done <- err
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

func TestWithRetryDisabledReturnsInner(t *testing.T) {
	f := &fakeStore{}
	if got := WithRetry(f, 1, time.Second); got != Store(f) {
		t.Error("WithRetry(attempts=1) wrapped the store; want the original back")
	}
}

func TestWithTelemetryToleratesNilMeter(t *testing.T) {
	f := &fakeStore{}
	s := WithTelemetry(f, nil)

	if _, err := s.Get(t.Context(), "k"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := f.calls.Load(); got != 1 {
		t.Errorf("made %d calls, want 1", got)
	}
}

// Telemetry must be transparent: it observes, it does not alter results.
func TestWithTelemetryPassesResultsThrough(t *testing.T) {
	boom := errors.New("backend down")
	f := &fakeStore{errs: []error{boom}}
	s := WithTelemetry(f, nil)

	if _, err := s.Get(t.Context(), "k"); !errors.Is(err, boom) {
		t.Errorf("Get error = %v, want %v", err, boom)
	}
}

func TestChainAppliesMiddlewareInOrder(t *testing.T) {
	f := &fakeStore{errs: []error{errors.New("transient")}}
	s := Chain(f, WithRetryMiddleware(2, time.Millisecond), WithTelemetryMiddleware(nil))

	if _, err := s.Get(t.Context(), "k"); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := f.calls.Load(); got != 2 {
		t.Errorf("made %d calls, want 2 — the retry middleware was not applied", got)
	}
}
