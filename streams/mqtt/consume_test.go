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

func TestHeadersSurviveARedelivery(t *testing.T) {
	_, m := declareWith(t, testStreams(t), streams.Stream{Name: "durable", Subjects: []string{subject}}.WithHeader("lang", "go"))
	d, _ := streams.AsDurable(m)

	first, cancelFirst := context.WithCancel(t.Context())
	ch, err := d.Consume(first, subject, "headers")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, perr := m.Publish(t.Context(), subject, event{User: "a"}, streams.Header("try", "1")); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}
	got := recv(t, ch, 10*time.Second)
	if got.Headers.Get("try") != "1" {
		t.Errorf("headers = %v, want try=1", got.Headers)
	}
	// Not acknowledged: the session keeps it and hands it back on resume.
	if got.CanDelayRedelivery() {
		t.Error("CanDelayRedelivery = true on MQTT")
	}
	if nerr := got.Nak(first); nerr != nil {
		t.Fatalf("Nak: %v", nerr)
	}
	cancelFirst()
	time.Sleep(500 * time.Millisecond)

	again, err := d.Consume(t.Context(), subject, "headers")
	if err != nil {
		t.Fatalf("Consume (again): %v", err)
	}
	redelivered := recv(t, again, 10*time.Second)
	if redelivered.Headers.Get("lang") != "go" || redelivered.Headers.Get("try") != "1" {
		t.Errorf("redelivered headers = %v, want lang=go try=1", redelivered.Headers)
	}
	if aerr := redelivered.Ack(t.Context()); aerr != nil {
		t.Errorf("Ack: %v", aerr)
	}
}

func TestDelayedNakIsRefusedByName(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)
	ch, err := d.Consume(t.Context(), subject, "delay")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, perr := m.Publish(t.Context(), subject, event{}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}
	got := recv(t, ch, 10*time.Second)
	if got.CanDelayRedelivery() || got.MaxRedeliveryDelay() != 0 {
		t.Error("MQTT claims it can delay a redelivery")
	}
	if nerr := got.NakAfter(t.Context(), 5*time.Second); !errors.Is(nerr, streams.ErrUnsupported) {
		t.Errorf("NakAfter = %v, want ErrUnsupported", nerr)
	}
}

// The session is named by consumer and subject, not stream, so the same name
// on another stream resumes the session the first one left, subscribed to the
// first stream's topic. It must still hear the new stream: a resumed session is
// subscribed again, which is what Go once skipped and runtime-rs does not.
func TestAResumedSessionIsSubscribedAgain(t *testing.T) {
	s := testStreams(t)
	_, first := declare(t, s, subject)
	_, second := declare(t, s, subject)

	ctx, cancel := context.WithCancel(t.Context())
	ch, err := mustDurable(t, first).Consume(ctx, subject, "ledger")
	if err != nil {
		t.Fatalf("Consume (first stream): %v", err)
	}
	if _, perr := first.Publish(t.Context(), subject, event{User: "first"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}
	if aerr := recv(t, ch, 10*time.Second).Ack(ctx); aerr != nil {
		t.Fatalf("Ack: %v", aerr)
	}
	time.Sleep(300 * time.Millisecond)
	cancel()
	time.Sleep(500 * time.Millisecond)

	moved, err := mustDurable(t, second).Consume(t.Context(), subject, "ledger")
	if err != nil {
		t.Fatalf("Consume (second stream): %v", err)
	}
	id, err := second.Publish(t.Context(), subject, event{User: "moved"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := recv(t, moved, 10*time.Second); got.ID != id {
		t.Errorf("delivered %q, want %q", got.ID, id)
	}
}

// A resumed session subscribes again while the broker is redelivering what it
// kept, and must still get all of it.
func TestAResumedSessionGetsItsBacklogWhileSubscribingAgain(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d := mustDurable(t, m)

	ctx, cancel := context.WithCancel(t.Context())
	if _, err := d.Consume(ctx, subject, "backlog"); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	cancel()
	time.Sleep(500 * time.Millisecond)

	const count = 20
	want := map[string]bool{}
	for i := range count {
		id, err := m.Publish(t.Context(), subject, event{User: fmt.Sprint(i)})
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		want[id] = true
	}
	again, err := d.Consume(t.Context(), subject, "backlog")
	if err != nil {
		t.Fatalf("Consume (resumed): %v", err)
	}
	for range count {
		got := recv(t, again, 10*time.Second)
		delete(want, got.ID)
		_ = got.Ack(t.Context())
	}
	if len(want) > 0 {
		t.Errorf("%d of the backlog never arrived", len(want))
	}
}

// A durable consumer is handed a retained value when its subscription is new,
// not again every time it resumes.
func TestARetainedValueReachesADurableConsumerOnce(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d := mustDurable(t, m)
	sent, err := m.Publish(t.Context(), subject, event{User: "last"}, streams.Retain())
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	ch, err := d.Consume(ctx, subject, "retained")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	got := recv(t, ch, 10*time.Second)
	if got.ID != sent {
		t.Errorf("delivered %q, want the retained %q", got.ID, sent)
	}
	_ = got.Ack(ctx)
	time.Sleep(300 * time.Millisecond)
	cancel()
	time.Sleep(500 * time.Millisecond)

	resumed, err := d.Consume(t.Context(), subject, "retained")
	if err != nil {
		t.Fatalf("Consume (resumed): %v", err)
	}
	quiet(t, resumed, time.Second, "a resumed durable consumer, of the retained value")
}

func mustDurable(t *testing.T, m streams.Manager) streams.Durable {
	t.Helper()

	d, err := streams.AsDurable(m)
	if err != nil {
		t.Fatalf("AsDurable: %v", err)
	}
	return d
}
