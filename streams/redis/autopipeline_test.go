package redis_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	streamsredis "github.com/the-protobuf-project/runtime-go/streams/redis"
)

// publishAll publishes n events at once from as many goroutines, so the
// auto-pipeliner has concurrent publishes to share pipelines between.
func publishAll(t *testing.T, m streams.Manager, n int) {
	t.Helper()
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			if _, err := m.Publish(t.Context(), subject, event{User: fmt.Sprint(i)}); err != nil {
				t.Errorf("Publish %d: %v", i, err)
			}
		})
	}
	wg.Wait()
}

// Concurrent publishes through the auto-pipeliner all land, once each, on a
// durable stream.
func TestAutoPipelinedAppendsLandOnce(t *testing.T) {
	s := newDurable(t, streamsredis.WithAutoPipeline())
	t.Cleanup(func() { _ = s.(streams.Closer).Close() })
	d := bindDurable(t, s, subject)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := d.Consume(ctx, subject, "workers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	m, err := s.Bind(ctx, streamOf(t, s))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	const n = 200
	publishAll(t, m, n)

	seen := map[string]bool{}
	for range n {
		got := recvDelivery(t, ch, 5*time.Second)
		var e event
		if derr := got.Decode(&e); derr != nil {
			t.Fatalf("Decode: %v", derr)
		}
		if seen[e.User] {
			t.Fatalf("event %s appended twice", e.User)
		}
		seen[e.User] = true
		if aerr := got.Ack(ctx); aerr != nil {
			t.Fatalf("Ack: %v", aerr)
		}
	}
}

// Over pub/sub the subscription stays on the client while publishes go through
// the auto-pipeliner, and closing the provider leaves a caller's client open.
func TestAutoPipelinedPublishesReachASubscriber(t *testing.T) {
	rdb := testClient(t)
	s := streamsredis.Use(rdb, streamsredis.WithPrefix(prefix()), streamsredis.WithAutoPipeline())
	_, m := bind(t, s, subject)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	const n = 200
	publishAll(t, m, n)
	seen := map[string]bool{}
	for range n {
		select {
		case msg := <-ch:
			var e event
			if derr := msg.Decode(&e); derr != nil {
				t.Fatalf("Decode: %v", derr)
			}
			seen[e.User] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("received %d of %d", len(seen), n)
		}
	}
	if len(seen) != n {
		t.Fatalf("received %d distinct events, want %d", len(seen), n)
	}

	if err := s.(streams.Closer).Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		t.Fatalf("the caller's client after Close: %v", err)
	}
}
