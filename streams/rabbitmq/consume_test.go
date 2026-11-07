package rabbitmq_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
)

// A durable queue but not a log: the same split as MQTT.
func TestRabbitIsDurableButNotPositioned(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)

	if _, err := streams.AsDurable(m); err != nil {
		t.Errorf("AsDurable on RabbitMQ: %v", err)
	}
	if _, err := streams.AsPositioned(m); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("AsPositioned on RabbitMQ = %v, want ErrUnsupported", err)
	}
}

func TestConsumeDeliversAndAcknowledges(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, err := streams.AsDurable(m)
	if err != nil {
		t.Fatalf("AsDurable: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := d.Consume(ctx, subject, "billing")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	want := event{User: "grace", Action: "created"}
	if _, perr := m.Publish(ctx, subject, want); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	got := recvDelivery(t, ch, 10*time.Second)
	if got.Attempt != 1 {
		t.Errorf("Attempt = %d on a first delivery, want 1", got.Attempt)
	}

	var decoded event
	if derr := got.Decode(&decoded); derr != nil {
		t.Fatalf("Decode: %v", derr)
	}
	if decoded != want {
		t.Errorf("decoded %+v, want %+v", decoded, want)
	}
	if aerr := got.Ack(ctx); aerr != nil {
		t.Fatalf("Ack: %v", aerr)
	}
}

// RabbitMQ is the only provider here with a true negative acknowledgement, so
// Nak returns a message immediately rather than after a timeout.
func TestNakReturnsTheMessageImmediately(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := d.Consume(ctx, subject, "billing")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, perr := m.Publish(ctx, subject, event{User: "ada"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	first := recvDelivery(t, ch, 10*time.Second)
	if nerr := first.Nak(ctx); nerr != nil {
		t.Fatalf("Nak: %v", nerr)
	}

	// No visibility timeout to wait out: the broker hands it straight back.
	second := recvDelivery(t, ch, 5*time.Second)
	if second.ID != first.ID {
		t.Errorf("got a different message back: %q then %q", first.ID, second.ID)
	}
	if second.Attempt < 2 {
		t.Errorf("Attempt = %d after a Nak, want at least 2", second.Attempt)
	}
	if aerr := second.Ack(ctx); aerr != nil {
		t.Fatalf("Ack: %v", aerr)
	}
}

// The queue is the broker's, so it holds messages while the consumer is away.
func TestAQueueKeepsMessagesWhileTheConsumerIsAway(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	// Attach once so the queue and its binding exist.
	first, cancelFirst := context.WithCancel(t.Context())
	ch, err := d.Consume(first, subject, "billing")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, perr := m.Publish(first, subject, event{User: "first"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}
	got := recvDelivery(t, ch, 10*time.Second)
	if aerr := got.Ack(first); aerr != nil {
		t.Fatalf("Ack: %v", aerr)
	}
	cancelFirst()
	time.Sleep(500 * time.Millisecond)

	// Published with nobody consuming. The queue holds it.
	if _, perr := m.Publish(t.Context(), subject, event{User: "second"}); perr != nil {
		t.Fatalf("Publish while detached: %v", perr)
	}

	second, cancelSecond := context.WithCancel(t.Context())
	defer cancelSecond()

	again, err := d.Consume(second, subject, "billing")
	if err != nil {
		t.Fatalf("Consume (second): %v", err)
	}
	resumed := recvDelivery(t, again, 10*time.Second)

	var decoded event
	if derr := resumed.Decode(&decoded); derr != nil {
		t.Fatalf("Decode: %v", derr)
	}
	if decoded.User != "second" {
		t.Errorf("resumed at %+v, want the message published while detached", decoded)
	}
	_ = resumed.Ack(second)
}

// Two consumers under one name take from one queue, so the work is split
// rather than doubled.
func TestOneNameSplitsTheWork(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	first, err := d.Consume(ctx, subject, "shared")
	if err != nil {
		t.Fatalf("Consume (first): %v", err)
	}
	second, err := d.Consume(ctx, subject, "shared")
	if err != nil {
		t.Fatalf("Consume (second): %v", err)
	}

	const total = 6
	for i := range total {
		if _, perr := m.Publish(ctx, subject, event{User: "u", Action: fmt.Sprint(i)}); perr != nil {
			t.Fatalf("Publish %d: %v", i, perr)
		}
	}

	seen := map[string]bool{}
	deadline := time.After(15 * time.Second)
	for len(seen) < total {
		select {
		case msg := <-first:
			seen[msg.ID] = true
			_ = msg.Ack(ctx)
		case msg := <-second:
			seen[msg.ID] = true
			_ = msg.Ack(ctx)
		case <-deadline:
			t.Fatalf("saw %d of %d messages across both consumers", len(seen), total)
		}
	}
}
