package rabbitmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	streamsrabbit "github.com/the-protobuf-project/runtime-go/streams/rabbitmq"
)

// The connection keeps itself up. These cut it from the broker's side and check
// the caller does not have to notice.

const quickly = 200 * time.Millisecond

func TestPublishSurvivesADroppedConnection(t *testing.T) {
	_, m := declare(t, testStreams(t, streamsrabbit.WithReconnectDelay(quickly)), subject)
	d, _ := streams.AsDurable(m)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	ch, err := d.Consume(ctx, subject, "audit")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	dropConnections(t)

	// Publish waits out the reconnection rather than failing into it.
	id, err := m.Publish(ctx, subject, event{User: "after"})
	if err != nil {
		t.Fatalf("Publish after a drop: %v", err)
	}
	if got := recv(t, ch, 15*time.Second); got.ID != id {
		t.Errorf("delivered %q, want %q", got.ID, id)
	}
}

// A durable queue outlives the connection. What was delivered and not
// acknowledged goes back to it when the connection drops, and an
// acknowledgement for it afterwards fails rather than settling some other
// message that happens to reuse its delivery tag.
func TestConsumerLosesNothingAcrossADroppedConnection(t *testing.T) {
	_, m := declare(t, testStreams(t, streamsrabbit.WithReconnectDelay(quickly)), subject)
	d, _ := streams.AsDurable(m)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	ch, err := d.Consume(ctx, subject, "billing")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	first, err := m.Publish(ctx, subject, event{User: "first"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	held := recv(t, ch, 10*time.Second)

	dropConnections(t)

	// Returning means the connection is back, so the held delivery's channel
	// is certainly gone.
	second, err := m.Publish(ctx, subject, event{User: "second"})
	if err != nil {
		t.Fatalf("Publish after a drop: %v", err)
	}
	if err := held.Ack(ctx); err == nil {
		t.Error("acknowledging a delivery from before the drop succeeded")
	}

	seen := map[string]streams.Delivery{}
	for len(seen) < 2 {
		got := recv(t, ch, 15*time.Second)
		seen[got.ID] = got
		if err := got.Ack(ctx); err != nil {
			t.Fatalf("Ack after reconnecting: %v", err)
		}
	}
	if again, ok := seen[first]; !ok || again.Attempt < 2 {
		t.Errorf("the held message came back as %+v, want it redelivered", again)
	}
	if _, ok := seen[second]; !ok {
		t.Errorf("the message published after the drop never arrived")
	}
}

// An undurable subscription's queue goes with the connection, so a new one is
// bound in its place.
func TestSubscribeSurvivesADroppedConnection(t *testing.T) {
	_, m := declare(t, testStreams(t, streamsrabbit.WithReconnectDelay(quickly)), subject)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	ch, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	dropConnections(t)

	// What is published before the new queue is bound is gone — that is what
	// undurable means — so keep publishing until one arrives.
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := m.Publish(ctx, subject, event{User: "again"}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		select {
		case <-ch:
			return
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal("the subscription never came back after its connection dropped")
		}
	}
}
