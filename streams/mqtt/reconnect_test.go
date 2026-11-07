package mqtt_test

import (
	"context"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	streamsmqtt "github.com/the-protobuf-project/runtime-go/streams/mqtt"
)

// Every connection keeps itself up. These drop one from the broker's side —
// what a network failure or a broker restart looks like to a client — and check
// the caller does not have to notice.

const quickly = 100 * time.Millisecond

func TestPublishSurvivesADroppedConnection(t *testing.T) {
	addr, srv := testBroker(t, broker{})
	s := connect(t, addr, streamsmqtt.WithClientID("pub-1"), streamsmqtt.WithReconnectDelay(quickly))
	_, m := declare(t, s, subject)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ch, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	drop(t, srv, "pub-1")

	// Publish waits out the reconnection rather than failing into it.
	id, err := m.Publish(ctx, subject, event{User: "after"})
	if err != nil {
		t.Fatalf("Publish after a drop: %v", err)
	}
	if got := recv(t, ch, 10*time.Second); got.ID != id {
		t.Errorf("delivered %q, want %q", got.ID, id)
	}
}

// An undurable subscription has nothing kept for it, so a reconnect finds no
// session and has to subscribe again on its own.
func TestSubscribeSurvivesADroppedConnection(t *testing.T) {
	addr, srv := testBroker(t, broker{})
	_, m := declare(t, connect(t, addr, streamsmqtt.WithReconnectDelay(quickly)), subject)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	ch, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	drop(t, srv, "runtime-go-sub-")

	// What is published while the subscription is down is gone — that is
	// what undurable means — so keep publishing until one arrives.
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

// A durable consumer's session outlives the connection, so what is published
// while it is down is waiting when the connection comes back.
func TestConsumerLosesNothingAcrossADroppedConnection(t *testing.T) {
	addr, srv := testBroker(t, broker{})
	_, m := declare(t, connect(t, addr, streamsmqtt.WithReconnectDelay(time.Second)), subject)
	d, _ := streams.AsDurable(m)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	ch, err := d.Consume(ctx, subject, "billing")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	drop(t, srv, "billing-")

	// The reconnect delay is long enough that this lands while it is away.
	id, err := m.Publish(ctx, subject, event{User: "while-down"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got := recv(t, ch, 10*time.Second)
	if got.ID != id {
		t.Errorf("delivered %q, want the message published while it was down, %q", got.ID, id)
	}
	if err := got.Ack(ctx); err != nil {
		t.Errorf("Ack after reconnecting: %v", err)
	}
}
