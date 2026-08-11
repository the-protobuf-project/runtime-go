package helpers

import (
	"context"
	"testing"
	"time"
)

func TestHandleContext_ReusesProvidedContext(t *testing.T) {
	origCtx, origCancel := context.WithCancel(context.Background())
	t.Cleanup(origCancel)

	in := NatsContext{Ctx: origCtx, CancelFunc: origCancel}

	got := HandleContext(in)

	if got.Ctx != origCtx {
		t.Fatalf("expected provided context to be reused")
	}
	if got.CancelFunc == nil {
		t.Fatalf("expected CancelFunc to be carried over")
	}
}

func TestHandleContext_WithFutureDeadline(t *testing.T) {
	deadline := time.Now().Add(80 * time.Millisecond)
	got := HandleContext(NatsContext{Time: deadline})
	if got.CancelFunc == nil {
		t.Fatalf("expected CancelFunc to be set")
	}
	t.Cleanup(got.CancelFunc)

	dl, ok := got.Ctx.Deadline()
	if !ok {
		t.Fatalf("expected deadline to be set")
	}
	// Allow a little scheduling wiggle room.
	diff := dl.Sub(deadline)
	if diff < -20*time.Millisecond || diff > 20*time.Millisecond {
		t.Fatalf("deadline mismatch: got=%v want≈%v (diff=%v)", dl, deadline, diff)
	}

	// Should not be done immediately.
	select {
	case <-got.Ctx.Done():
		t.Fatalf("context should not be done yet")
	default:
	}

	// Should be done after the deadline passes.
	select {
	case <-got.Ctx.Done():
		// ok if it fires a tad early/late
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("context did not expire after expected time")
	}
}

func TestHandleContext_WithPastDeadline(t *testing.T) {
	deadline := time.Now().Add(-50 * time.Millisecond)
	got := HandleContext(NatsContext{Time: deadline})
	if got.CancelFunc == nil {
		t.Fatalf("expected CancelFunc to be set")
	}
	t.Cleanup(got.CancelFunc)

	// Should be already done (past deadline).
	select {
	case <-got.Ctx.Done():
		// ok
	default:
		t.Fatalf("context should already be done for past deadline")
	}
}

func TestHandleContext_DefaultBackground(t *testing.T) {
	got := HandleContext()
	if got.Ctx == nil {
		t.Fatalf("expected non-nil context")
	}
	if got.CancelFunc == nil {
		t.Fatalf("expected non-nil CancelFunc")
	}

	// No deadline expected.
	if _, ok := got.Ctx.Deadline(); ok {
		t.Fatalf("did not expect a deadline")
	}

	// Cancel should close Done.
	done := make(chan struct{})
	go func() {
		<-got.Ctx.Done()
		close(done)
	}()
	got.CancelFunc()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("context did not cancel in time")
	}
}
