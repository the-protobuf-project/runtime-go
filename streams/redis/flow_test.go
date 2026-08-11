package redis_test

// Flow control.
//
// What is deliberately *not* asserted here: that buffering stops a slow reader
// from stalling another subscription. go-redis opens a dedicated connection per
// Subscribe and buffers roughly a hundred messages inside it, so on this
// provider the delivery channel's own depth is not observable at these sizes —
// a test claiming otherwise passes whether or not the buffer exists, which is
// worse than no test. The knob still matters: it is what a caller sets when the
// client underneath does not buffer, and what bounds unacknowledged work on a
// durable consumer.

import (
	"context"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
)

// Prefetch is a request, and asking for none has to mean none — a synchronous
// hand-off is a legitimate thing to want when ordering matters more than
// throughput.
func TestPrefetchIsHonored(t *testing.T) {
	s := newStreams(t)
	_, m := bind(t, s, subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	for _, prefetch := range []int{-1, 1, 128} {
		ch, err := m.Subscribe(ctx, subject, streams.Prefetch(prefetch))
		if err != nil {
			t.Fatalf("Subscribe with Prefetch(%d): %v", prefetch, err)
		}
		if _, perr := m.Publish(ctx, subject, event{User: "ada"}); perr != nil {
			t.Fatalf("Publish: %v", perr)
		}
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("nothing was delivered with Prefetch(%d)", prefetch)
		}
	}
}

// A subscription still works when a message arrives before the reader does,
// which is the case the buffer exists for.
func TestDeliverySurvivesALateReader(t *testing.T) {
	s := newStreams(t)
	_, m := bind(t, s, subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := m.Subscribe(ctx, subject, streams.Prefetch(8))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, perr := m.Publish(ctx, subject, event{User: "early"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	// The reader arrives well after the message did.
	time.Sleep(500 * time.Millisecond)

	select {
	case msg := <-ch:
		var got event
		if derr := msg.Decode(&got); derr != nil {
			t.Fatalf("Decode: %v", derr)
		}
		if got.User != "early" {
			t.Errorf("received %+v, want the message published before the read", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a message published before the reader arrived was lost")
	}
}
