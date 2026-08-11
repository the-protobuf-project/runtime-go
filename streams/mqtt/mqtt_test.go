package mqtt_test

// These run against an MQTT broker started in-process by mochi-mqtt, so they
// need no broker installed and no container.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/the-protobuf-project/runtime-go/streams"
	streamsmqtt "github.com/the-protobuf-project/runtime-go/streams/mqtt"
)

type event struct {
	User   string `json:"user"`
	Action string `json:"action"`
}

const subject = "user/created"

// testBroker starts a broker on a free port and returns its address.
func testBroker(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	srv := mochi.New(&mochi.Options{InlineClient: true})
	if err := srv.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatalf("AddHook: %v", err)
	}
	if err := srv.AddListener(listeners.NewTCP(listeners.Config{ID: "t", Address: addr})); err != nil {
		t.Fatalf("AddListener: %v", err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { _ = srv.Close() })

	// Wait for it to accept, so Connect does not race the listener.
	for range 50 {
		d := net.Dialer{Timeout: 200 * time.Millisecond}
		if c, derr := d.DialContext(t.Context(), "tcp", addr); derr == nil {
			_ = c.Close()
			return addr
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the broker never came up")
	return ""
}

func testStreams(t *testing.T, opts ...streamsmqtt.Option) streams.Streams {
	t.Helper()

	s, err := streamsmqtt.Connect(t.Context(), testBroker(t), opts...)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() {
		if c, ok := s.(streams.Closer); ok {
			_ = c.Close()
		}
	})
	return s
}

func declare(t *testing.T, s streams.Streams, subjects ...string) (streams.Stream, streams.Manager) {
	t.Helper()

	stream, err := s.Create(t.Context(), streams.Stream{Name: "test", Subjects: subjects})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := s.Bind(t.Context(), stream.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return stream, m
}

func recvDelivery(t *testing.T, ch <-chan streams.Delivery, within time.Duration) streams.Delivery {
	t.Helper()

	select {
	case d, ok := <-ch:
		if !ok {
			t.Fatal("delivery channel closed before a message arrived")
		}
		return d
	case <-time.After(within):
		t.Fatalf("no delivery within %s", within)
		return streams.Delivery{}
	}
}

func TestLifecycle(t *testing.T) {
	s := testStreams(t)
	ctx := t.Context()

	created, err := s.Create(ctx, streams.Stream{
		Name: "users", Description: "before", Subjects: []string{subject},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.ID == "" {
		t.Fatal("Create did not assign an id")
	}

	if _, uerr := s.Update(ctx, created.ID, streams.Stream{
		Name: "users", Description: "after", Subjects: []string{subject},
	}); uerr != nil {
		t.Fatalf("Update: %v", uerr)
	}
	if after, _ := s.Get(ctx, created.ID); after.Description != "after" {
		t.Errorf("Update did not take: %q", after.Description)
	}

	list, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("List returned %d streams, want 1", len(list))
	}

	if derr := s.Delete(ctx, created.ID); derr != nil {
		t.Fatalf("Delete: %v", derr)
	}
	if _, gerr := s.Get(ctx, created.ID); !errors.Is(gerr, streams.ErrNotFound) {
		t.Errorf("Get after Delete = %v, want ErrNotFound", gerr)
	}
}

func TestPublishAndSubscribe(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	want := event{User: "ada", Action: "created"}
	id, err := m.Publish(ctx, subject, want)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-ch:
		if msg.ID != id {
			t.Errorf("delivered id %q, want the published %q", msg.ID, id)
		}
		var got event
		if derr := msg.Decode(&got); derr != nil {
			t.Fatalf("Decode: %v", derr)
		}
		if got != want {
			t.Errorf("decoded %+v, want %+v", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("nothing was delivered")
	}
}

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
	got := recvDelivery(t, ch, 10*time.Second)
	if aerr := got.Ack(first); aerr != nil {
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
	resumed := recvDelivery(t, again, 10*time.Second)

	var decoded event
	if derr := resumed.Decode(&decoded); derr != nil {
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

// A stream may declare a filter; a message still has to land somewhere specific.
func TestWildcardSubjects(t *testing.T) {
	_, m := declare(t, testStreams(t), "user/+")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := m.Subscribe(ctx, "user/+")
	if err != nil {
		t.Fatalf("Subscribe to the filter: %v", err)
	}
	if _, perr := m.Publish(ctx, "user/created", event{User: "ada"}); perr != nil {
		t.Fatalf("Publish to a subject the filter covers: %v", perr)
	}

	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("nothing was delivered")
	}

	if _, perr := m.Publish(ctx, "user/+", event{}); !errors.Is(perr, streams.ErrUnknownSubject) {
		t.Errorf("Publish to a wildcard = %v, want ErrUnknownSubject", perr)
	}
}

func TestPublishRejectsAnUndeclaredSubject(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)

	if _, err := m.Publish(t.Context(), "typo", event{}); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Publish = %v, want ErrUnknownSubject", err)
	}
}

func TestPublishRejectsATTL(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)

	_, err := m.Publish(t.Context(), subject, event{}, streams.TTL(time.Second))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with a TTL = %v, want ErrUnsupported", err)
	}
}

func TestConsumeRequiresAConsumerName(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	if _, err := d.Consume(t.Context(), subject, ""); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume without a name = %v, want ErrUnsupported", err)
	}
}

func TestConnectRejectsAnEmptyAddress(t *testing.T) {
	if _, err := streamsmqtt.Connect(t.Context(), ""); err == nil {
		t.Error("Connect accepted an empty address")
	}
}
