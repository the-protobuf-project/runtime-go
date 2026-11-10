package mqtt_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/the-protobuf-project/runtime-go/streams/core"

	"github.com/the-protobuf-project/runtime-go/streams"
	streamsmqtt "github.com/the-protobuf-project/runtime-go/streams/mqtt"
)

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

	msg := recv(t, ch, 10*time.Second)
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
	// The subject is the topic the message arrived on, not the filter.
	if got := recv(t, ch, 10*time.Second); got.Subject != "user/created" {
		t.Errorf("Subject = %q, want user/created", got.Subject)
	}

	_, perr := m.Publish(ctx, "user/+", event{})
	if !errors.Is(perr, streams.ErrUnknownSubject) {
		t.Errorf("Publish to a wildcard = %v, want ErrUnknownSubject", perr)
	}
	if !errors.Is(perr, streams.ErrUnsupported) {
		t.Errorf("Publish to a wildcard = %v, want ErrUnsupported too", perr)
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

func TestConnectRejectsAnEmptyAddress(t *testing.T) {
	if _, err := streamsmqtt.Connect(t.Context(), ""); err == nil {
		t.Error("Connect accepted an empty address")
	}
}

// Connect makes the first connection itself, so an unreachable broker is an
// error at the call rather than a connection retrying in the background.
func TestConnectFailsWhenNothingIsListening(t *testing.T) {
	// A port bound and released, so nothing is listening on it.
	l, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	if _, err := streamsmqtt.Connect(t.Context(), addr); err == nil {
		t.Error("Connect succeeded against a closed broker")
	}
}

func TestSubscribeWithAGroupSharesTheSubject(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	first, err := m.Subscribe(ctx, subject, streams.Group("readers"))
	if err != nil {
		t.Fatalf("Subscribe (first): %v", err)
	}
	second, err := m.Subscribe(ctx, subject, streams.Group("readers"))
	if err != nil {
		t.Fatalf("Subscribe (second): %v", err)
	}
	for i := range 4 {
		if _, perr := m.Publish(ctx, subject, event{User: "u", Action: fmt.Sprint(i)}); perr != nil {
			t.Fatalf("Publish: %v", perr)
		}
	}
	seen := 0
	deadline := time.After(10 * time.Second)
	for seen < 4 {
		select {
		case <-first:
			seen++
		case <-second:
			seen++
		case <-deadline:
			t.Fatalf("saw %d of 4", seen)
		}
	}
	quiet(t, first, 300*time.Millisecond, "the first reader, after the subject was shared out")
	quiet(t, second, 300*time.Millisecond, "the second reader, after the subject was shared out")
}

func TestCancelingASubscriptionStopsIt(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)

	first, cancelFirst := context.WithCancel(t.Context())
	ch, err := m.Subscribe(first, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cancelFirst()
	for msg := range ch {
		_ = msg // drained until it closes
	}

	// A second subscriber still gets everything, and publishing to a topic
	// whose old subscriber is gone does not fail or stall.
	again, err := m.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe (again): %v", err)
	}
	for range 3 {
		if _, perr := m.Publish(t.Context(), subject, event{User: "ada"}); perr != nil {
			t.Fatalf("Publish: %v", perr)
		}
	}
	for range 3 {
		recv(t, again, 10*time.Second)
	}
}

func TestPublishBatchPublishesEach(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	b, err := streams.AsBatch(m)
	if err != nil {
		t.Fatalf("AsBatch: %v", err)
	}
	ch, err := m.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	ids, err := b.PublishBatch(t.Context(), subject, []any{event{User: "a"}, event{User: "b"}, event{User: "c"}})
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("PublishBatch returned %d ids, want 3", len(ids))
	}
	for _, id := range ids {
		if got := recv(t, ch, 10*time.Second); got.ID != id {
			t.Errorf("delivered %q, want %q in batch order", got.ID, id)
		}
	}

	if _, err := b.PublishBatch(t.Context(), subject, []any{event{}}, streams.ID("x")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("PublishBatch with one id = %v, want ErrUnsupported", err)
	}
	if _, err := b.PublishBatch(t.Context(), "typo", []any{event{}}); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("PublishBatch on an undeclared subject = %v, want ErrUnknownSubject", err)
	}
}

// Publishes on one topic reach a subscriber in the order they were sent over
// several publishing connections, because the topic takes one connection: a
// pipelined batch, with every message in flight at once, and publishes made
// one after another alike.
func TestPublishesOnATopicKeepTheirOrderOverSeveralConnections(t *testing.T) {
	_, m := declare(t, testStreams(t, streamsmqtt.WithPublishConnections(4)), subject, "other/+")
	b, _ := streams.AsBatch(m)
	ch, err := m.Subscribe(t.Context(), subject, streams.Prefetch(512))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	const count = 200
	values := make([]any, count)
	for i := range values {
		values[i] = event{User: fmt.Sprint(i)}
	}
	sent, err := b.PublishBatch(t.Context(), subject, values)
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	for i := range 20 {
		id, perr := m.Publish(t.Context(), subject, event{User: "single"}, streams.ID(fmt.Sprintf("m%04d", i)))
		if perr != nil {
			t.Fatalf("Publish: %v", perr)
		}
		sent = append(sent, id)
	}
	// Other topics take other connections, concurrently.
	for i := range 8 {
		if _, perr := m.Publish(t.Context(), fmt.Sprintf("other/%d", i), event{}); perr != nil {
			t.Fatalf("Publish on another topic: %v", perr)
		}
	}
	for i, want := range sent {
		if got := recv(t, ch, 10*time.Second); got.ID != want {
			t.Fatalf("message %d is %q, want %q", i, got.ID, want)
		}
	}
}

func TestPublishingAfterCloseFails(t *testing.T) {
	s := testStreams(t)
	_, m := declare(t, s, subject)
	if err := s.(streams.Closer).Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := m.Publish(ctx, subject, event{}); err == nil {
		t.Error("Publish after Close succeeded")
	}
}

// Headers, expiry, retain and delayed nak.

func TestHeadersTravelAsUserProperties(t *testing.T) {
	addr, _ := testBroker(t, broker{})
	stream, m := declareWith(t, connect(t, addr), streams.Stream{Name: "headers", Subjects: []string{subject}}.
		WithHeader("lang", "go").WithHeader("site", "plant-3"))
	_, wire := raw(t, addr, stream.ID+"/"+subject)
	ch, err := m.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	sent, err := m.Publish(t.Context(), subject, event{User: "ada"},
		streams.Header("site", "plant-4"), streams.Header("traceparent", "00-ab"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg := recv(t, ch, 10*time.Second)
	if msg.ID != sent {
		t.Errorf("delivered %q, want %q", msg.ID, sent)
	}
	want := streams.Headers{"lang": "go", "site": "plant-4", "traceparent": "00-ab"}
	if len(msg.Headers) != len(want) {
		t.Errorf("headers = %v, want %v", msg.Headers, want)
	}
	for k, v := range want {
		if msg.Headers.Get(k) != v {
			t.Errorf("header %s = %q, want %q (stream default, publish wins)", k, msg.Headers.Get(k), v)
		}
	}

	// On the wire: user properties in key order, and a version-1 frame with
	// nothing of the headers in it, so a reader that knows no headers reads it.
	p := recv(t, wire, 10*time.Second)
	if p.Properties == nil {
		t.Fatal("no properties on the wire")
	}
	keys := make([]string, 0, len(p.Properties.User))
	for _, u := range p.Properties.User {
		keys = append(keys, u.Key+"="+u.Value)
	}
	if got := strings.Join(keys, " "); got != "lang=go site=plant-4 traceparent=00-ab" {
		t.Errorf("user properties = %s", got)
	}
	if p.Payload[0] != 0xB7 || p.Payload[1] != 1 {
		t.Errorf("payload starts % x, want a version-1 frame", p.Payload[:2])
	}
	if unframed, err := core.Unpack(nil, subject, p.Payload); err != nil || unframed.Headers != nil {
		t.Errorf("headers inside the payload: %v, %v", unframed.Headers, err)
	}
}

func TestAMessageWithoutHeadersIsWhatGoAlwaysSent(t *testing.T) {
	addr, _ := testBroker(t, broker{})
	stream, m := declareWith(t, connect(t, addr), streams.Stream{Name: "plain", Subjects: []string{subject}})
	_, wire := raw(t, addr, stream.ID+"/"+subject)
	ch, err := m.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, perr := m.Publish(t.Context(), subject, event{User: "go"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}
	if got := recv(t, ch, 10*time.Second); got.Headers != nil {
		t.Errorf("headers = %v, want none", got.Headers)
	}
	// What is sent carries no properties at all (see
	// TestPropertiesCarryHeadersAndExpiryOnlyWhenSet); mochi adds its own
	// expiry when it forwards, so only the user properties are checked here.
	if p := recv(t, wire, 10*time.Second); p.Properties != nil && len(p.Properties.User) > 0 {
		t.Errorf("properties on a header-less publish: %+v", p.Properties)
	}

	// A message another MQTT 5 client sends, with user properties of its own,
	// arrives carrying them.
	other, _ := raw(t, addr, "")
	frame, err := core.Pack(nil, "from-rust", event{User: "rust"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Publish(t.Context(), &paho.Publish{
		Topic: stream.ID + "/" + subject, QoS: 1, Payload: frame,
		Properties: &paho.PublishProperties{User: paho.UserProperties{{Key: "lang", Value: "rust"}}},
	}); err != nil {
		t.Fatalf("raw Publish: %v", err)
	}
	got := recv(t, ch, 10*time.Second)
	if got.ID != "from-rust" || got.Headers.Get("lang") != "rust" {
		t.Errorf("delivered %q with %v, want from-rust with lang=rust", got.ID, got.Headers)
	}
}

func TestABatchCarriesTheHeadersOnEveryMessage(t *testing.T) {
	_, m := declareWith(t, testStreams(t), streams.Stream{Name: "batch", Subjects: []string{subject}}.WithHeader("lang", "go"))
	b, _ := streams.AsBatch(m)
	ch, err := m.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	ids, err := b.PublishBatch(t.Context(), subject, []any{event{User: "a"}, event{User: "b"}}, streams.Header("n", "1"))
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	for _, id := range ids {
		got := recv(t, ch, 10*time.Second)
		if got.ID != id || got.Headers.Get("lang") != "go" || got.Headers.Get("n") != "1" {
			t.Errorf("delivered %q with %v, want %q with lang=go n=1", got.ID, got.Headers, id)
		}
	}
}

func TestInvalidHeadersAreRefusedAtCreateUpdateAndPublish(t *testing.T) {
	s := testStreams(t)
	if _, err := s.Create(t.Context(), streams.Stream{Name: "bad", Subjects: []string{subject}}.WithHeader("has space", "x")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Create with a bad header = %v, want ErrInvalidHeader", err)
	}
	stream, m := declareWith(t, s, streams.Stream{Name: "good", Subjects: []string{subject}})
	if _, err := s.Update(t.Context(), stream.ID, streams.Stream{Name: "good", Subjects: []string{subject}}.WithHeader("k", "two\nlines")); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Update with a bad header = %v, want ErrInvalidHeader", err)
	}
	b, _ := streams.AsBatch(m)
	for _, opt := range []streams.Option{
		streams.Header("", "x"),
		streams.Header("a:b", "x"),
		streams.Header("k", strings.Repeat("v", 64*1024)),
	} {
		if _, err := m.Publish(t.Context(), subject, event{}, opt); !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("Publish = %v, want ErrInvalidHeader", err)
		}
		if _, err := b.PublishBatch(t.Context(), subject, []any{event{}}, opt); !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("PublishBatch = %v, want ErrInvalidHeader", err)
		}
	}
}

func TestExpiryIsTheMessageExpiryInterval(t *testing.T) {
	addr, _ := testBroker(t, broker{})
	stream, m := declareWith(t, connect(t, addr), streams.Stream{Name: "expiry", Subjects: []string{subject}})
	_, wire := raw(t, addr, stream.ID+"/"+subject)
	if _, err := m.Publish(t.Context(), subject, event{}, streams.Expires(1500*time.Millisecond)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	p := recv(t, wire, 10*time.Second)
	if p.Properties == nil || p.Properties.MessageExpiry == nil {
		t.Fatal("no expiry interval on the wire")
	}
	// Rounded up to whole seconds; a broker forwarding it may count down the
	// time it held the message.
	if got := *p.Properties.MessageExpiry; got < 1 || got > 2 {
		t.Errorf("expiry interval = %d, want 1 or 2", got)
	}
}

func TestARetainedMessageReachesALaterSubscriberAndCanBeCleared(t *testing.T) {
	s := testStreams(t)
	stream, m := declareWith(t, s, streams.Stream{Name: "retain", Subjects: []string{subject}})
	sent, err := m.Publish(t.Context(), subject, event{User: "last"}, streams.Retain(), streams.Header("lang", "go"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Subscribed after the publish, and still handed it, decoded.
	late, err := m.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	got := recv(t, late, 10*time.Second)
	var decoded event
	if derr := got.Decode(&decoded); derr != nil || got.ID != sent || decoded.User != "last" || got.Headers.Get("lang") != "go" {
		t.Errorf("retained %q %+v %v (%v), want %q from last with lang=go", got.ID, decoded, got.Headers, derr, sent)
	}

	// Cleared: the empty retained publish reaches the live subscriber and is
	// skipped, and a subscriber joining later gets nothing.
	if cerr := streamsmqtt.ClearRetained(t.Context(), s, stream.ID, subject); cerr != nil {
		t.Fatalf("ClearRetained: %v", cerr)
	}
	after, err := m.Publish(t.Context(), subject, event{User: "next"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := recv(t, late, 10*time.Second); got.ID != after {
		t.Errorf("delivered %q after the clear, want %q: the clear was not skipped", got.ID, after)
	}
	later, err := m.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	quiet(t, later, 500*time.Millisecond, "a subscriber after the clear")

	if err := streamsmqtt.ClearRetained(t.Context(), s, "nope", subject); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("ClearRetained on an undeclared stream = %v, want ErrNotFound", err)
	}
}
