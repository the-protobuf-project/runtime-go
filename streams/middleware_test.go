package streams

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// fakePublisher counts calls and returns whatever the test scripted.
type fakePublisher struct {
	calls atomic.Int64
	errs  []error // consumed one per call; nil once exhausted
}

func (f *fakePublisher) Publish(_ context.Context, _ string, _ any, opts ...Option) (string, error) {
	n := f.calls.Add(1)
	if int(n) <= len(f.errs) {
		return "", f.errs[n-1]
	}
	// Honor a caller-chosen id, as a real provider does, so decorators that
	// report the id used are exercised against both paths.
	if id := NewOptions(opts...).ID; id != "" {
		return id, nil
	}
	return "generated", nil
}

func TestWithPublisherRetryRetriesUntilSuccess(t *testing.T) {
	boom := errors.New("transient")
	f := &fakePublisher{errs: []error{boom, boom}} // third call succeeds
	p := WithPublisherRetry(f, 3, time.Millisecond)

	if _, err := p.Publish(t.Context(), "s", nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := f.calls.Load(); got != 3 {
		t.Errorf("made %d calls, want 3", got)
	}
}

func TestWithPublisherRetryGivesUpAfterAttempts(t *testing.T) {
	boom := errors.New("persistent")
	f := &fakePublisher{errs: []error{boom, boom, boom, boom}}
	p := WithPublisherRetry(f, 3, time.Millisecond)

	if _, err := p.Publish(t.Context(), "s", nil); !errors.Is(err, boom) {
		t.Fatalf("Publish error = %v, want %v", err, boom)
	}
	if got := f.calls.Load(); got != 3 {
		t.Errorf("made %d calls, want exactly 3", got)
	}
}

// The subject will not become valid on a second attempt.
func TestWithPublisherRetryDoesNotRetryUnknownSubject(t *testing.T) {
	f := &fakePublisher{errs: []error{ErrUnknownSubject, ErrUnknownSubject}}
	p := WithPublisherRetry(f, 3, time.Millisecond)

	if _, err := p.Publish(t.Context(), "s", nil); !errors.Is(err, ErrUnknownSubject) {
		t.Fatalf("Publish error = %v, want ErrUnknownSubject", err)
	}
	if got := f.calls.Load(); got != 1 {
		t.Errorf("made %d calls, want 1", got)
	}
}

func TestWithPublisherRetryStopsOnContextCancellation(t *testing.T) {
	boom := errors.New("transient")
	f := &fakePublisher{errs: []error{boom, boom, boom, boom, boom}}
	p := WithPublisherRetry(f, 5, time.Hour) // would sleep forever if ctx were ignored

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	done := make(chan error, 1)
	go func() { _, err := p.Publish(ctx, "s", nil); done <- err }()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want it to join context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retry slept through a canceled context")
	}
}

func TestWithPublisherRetryDisabledReturnsInner(t *testing.T) {
	f := &fakePublisher{}
	if got := WithPublisherRetry(f, 1, time.Second); got != Publisher(f) {
		t.Error("WithPublisherRetry(attempts=1) wrapped the publisher; want the original back")
	}
}

func TestWithPublisherTelemetryToleratesNilMeter(t *testing.T) {
	f := &fakePublisher{}
	p := WithPublisherTelemetry(f, nil)

	if _, err := p.Publish(t.Context(), "s", nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := f.calls.Load(); got != 1 {
		t.Errorf("made %d calls, want 1", got)
	}
}

// Telemetry must be transparent: it observes, it does not alter results.
func TestWithPublisherTelemetryPassesResultsThrough(t *testing.T) {
	boom := errors.New("broker down")
	f := &fakePublisher{errs: []error{boom}}
	p := WithPublisherTelemetry(f, nil)

	if _, err := p.Publish(t.Context(), "s", nil); !errors.Is(err, boom) {
		t.Errorf("Publish error = %v, want %v", err, boom)
	}
}

func TestChainPublisherAppliesMiddlewareInOrder(t *testing.T) {
	f := &fakePublisher{errs: []error{errors.New("transient")}}
	p := ChainPublisher(f,
		WithPublisherRetryMiddleware(2, time.Millisecond),
		WithPublisherTelemetryMiddleware(nil),
	)

	if _, err := p.Publish(t.Context(), "s", nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := f.calls.Load(); got != 2 {
		t.Errorf("made %d calls, want 2 — the retry middleware was not applied", got)
	}
}
