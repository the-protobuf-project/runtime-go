package mqtt_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
)

// MQTT is durable but has no log to seek, which is exactly why the contract
// keeps the two capabilities apart.
func TestMQTTIsDurableButNotPositioned(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)

	if _, err := streams.AsDurable(m); err != nil {
		t.Errorf("AsDurable on MQTT: %v", err)
	}
	if _, err := streams.AsPositioned(m); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("AsPositioned on MQTT = %v, want ErrUnsupported", err)
	}
}

func TestConsumeDeliversAndAcknowledges(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

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

	got := recv(t, ch, 10*time.Second)
	var decoded event
	if derr := got.Decode(&decoded); derr != nil {
		t.Fatalf("Decode: %v", derr)
	}
	if decoded != want {
		t.Errorf("decoded %+v, want %+v", decoded, want)
	}
	// MQTT flags a repeat but does not count them, and the contract asks for
	// zero rather than a number that would be invented.
	if got.Attempt != 0 {
		t.Errorf("Attempt = %d, want 0 where the provider cannot count", got.Attempt)
	}
	if aerr := got.Ack(ctx); aerr != nil {
		t.Fatalf("Ack: %v", aerr)
	}
}

// The session is the name: a message published while the consumer was away is
// waiting when it comes back.
func TestASessionKeepsMessagesWhileTheConsumerIsAway(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	// Attach once so the broker creates the session and its subscription.
	first, cancelFirst := context.WithCancel(t.Context())
	ch, err := d.Consume(first, subject, "billing")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, perr := m.Publish(first, subject, event{User: "first"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}
	if aerr := recv(t, ch, 10*time.Second).Ack(first); aerr != nil {
		t.Fatalf("Ack: %v", aerr)
	}
	// The acknowledgement is a packet on the wire, and disconnecting in the
	// same breath can close the connection before it is written.
	time.Sleep(300 * time.Millisecond)
	cancelFirst()
	time.Sleep(500 * time.Millisecond) // let the disconnect land

	// Published with nobody attached. A clean session would drop this.
	if _, perr := m.Publish(t.Context(), subject, event{User: "second"}); perr != nil {
		t.Fatalf("Publish while detached: %v", perr)
	}

	second, cancelSecond := context.WithCancel(t.Context())
	defer cancelSecond()

	again, err := d.Consume(second, subject, "billing")
	if err != nil {
		t.Fatalf("Consume (second): %v", err)
	}
	var decoded event
	if derr := recv(t, again, 10*time.Second).Decode(&decoded); derr != nil {
		t.Fatalf("Decode: %v", derr)
	}
	if decoded.User != "second" {
		t.Errorf("resumed at %+v, want the message published while detached", decoded)
	}
}

// A shared subscription is how MQTT splits work, and it is what Group means
// here — unlike the other providers, where the consumer name is the group.
func TestGroupSharesTheSubject(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	first, err := d.Consume(ctx, subject, "worker-a", streams.Group("billing"))
	if err != nil {
		t.Fatalf("Consume (first): %v", err)
	}
	second, err := d.Consume(ctx, subject, "worker-b", streams.Group("billing"))
	if err != nil {
		t.Fatalf("Consume (second): %v", err)
	}

	const total = 6
	for i := range total {
		if _, perr := m.Publish(ctx, subject, event{User: "u", Action: fmt.Sprint(i)}); perr != nil {
			t.Fatalf("Publish %d: %v", i, perr)
		}
	}

	seen := 0
	deadline := time.After(15 * time.Second)
	for seen < total {
		select {
		case msg := <-first:
			seen++
			_ = msg.Ack(ctx)
		case msg := <-second:
			seen++
			_ = msg.Ack(ctx)
		case <-deadline:
			t.Fatalf("saw %d of %d messages across the group", seen, total)
		}
	}
}

func TestConsumeRequiresAConsumerName(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	if _, err := d.Consume(t.Context(), subject, ""); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume without a name = %v, want ErrUnsupported", err)
	}
}
