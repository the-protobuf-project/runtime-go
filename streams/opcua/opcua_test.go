package opcua_test

import (
	"context"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
)

type event struct {
	User   string `json:"user"`
	Action string `json:"action"`
}

func TestSubscribeStartsWithTheCurrentValue(t *testing.T) {
	_, m := connect(t)
	msgs := subscribe(t, m, temperature)

	var got float64
	if err := next(t, msgs).Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != 0 {
		t.Fatalf("first value = %v, want the node's initial 0", got)
	}
}

func TestNativeValueRoundTrip(t *testing.T) {
	_, m := connect(t)
	ctx := t.Context()
	msgs := subscribe(t, m, temperature)
	next(t, msgs) // the current value

	if _, err := m.Publish(ctx, temperature, 21.5); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg := next(t, msgs)
	var got float64
	if err := msg.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != 21.5 || msg.Subject != temperature {
		t.Fatalf("got %v on %q, want 21.5 on %q", got, msg.Subject, temperature)
	}
}

func TestFramedValueRoundTrip(t *testing.T) {
	_, m := connect(t)
	ctx := t.Context()
	msgs := subscribe(t, m, payload)
	next(t, msgs) // the current, empty, value

	want := event{User: "ada", Action: "created"}
	id, err := m.Publish(ctx, payload, want)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg := next(t, msgs)
	if msg.ID != id {
		t.Fatalf("id = %q, want the published %q", msg.ID, id)
	}
	var got event
	if err := msg.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestPublishBatchWritesInOrder(t *testing.T) {
	_, m := connect(t)
	ctx := t.Context()
	msgs := subscribe(t, m, temperature)
	next(t, msgs)

	b, err := streams.AsBatch(m)
	if err != nil {
		t.Fatalf("AsBatch: %v", err)
	}
	ids, err := b.PublishBatch(ctx, temperature, []any{1.0, 2.0, 3.0})
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("got %d ids, want 3", len(ids))
	}

	// A node is a state, so what is guaranteed is that the last write is what
	// it ends up holding; earlier ones arrive only if the queue kept them.
	var last float64
	for last != 3.0 {
		if err := next(t, msgs).Decode(&last); err != nil {
			t.Fatalf("Decode: %v", err)
		}
	}
}

func TestCancelClosesTheChannel(t *testing.T) {
	_, m := connect(t)
	ctx, cancel := context.WithCancel(context.Background())

	msgs, err := m.Subscribe(ctx, temperature)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	next(t, msgs)
	cancel()

	deadline := time.After(10 * time.Second)
	for {
		select {
		case _, ok := <-msgs:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("the channel stayed open after its context was canceled")
		}
	}
}
