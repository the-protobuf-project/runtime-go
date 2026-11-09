//go:build live

package mqtt_test

// These run against the plaintext Docker broker, an independent MQTT 5
// implementation beside the in-process mochi the untagged tests use. Start it
// from streams/ with
//
//	docker compose -p tpp-streams-live-mqtt -f docker/compose.mqtt.yaml up -d --wait
//
// then run go test -race -tags live ./mqtt -run Live. MQTT_LIVE_ADDR (default
// 127.0.0.1:11883) points them elsewhere. They skip when nothing is listening.

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	streamsmqtt "github.com/the-protobuf-project/runtime-go/streams/mqtt"
)

// within is how long a live test waits for one message.
const within = 10 * time.Second

// liveAddr is the plaintext broker's address, or a skip when it is not up.
func liveAddr(t *testing.T) string {
	t.Helper()

	addr := os.Getenv("MQTT_LIVE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:11883"
	}
	reachable(t, addr)
	return addr
}

// reachable skips the test when nothing accepts a connection at addr.
func reachable(t *testing.T, addr string) {
	t.Helper()

	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Skipf("no broker at %s: %v", addr, err)
	}
	_ = conn.Close()
}

// fresh is a topic prefix no earlier run used, so sessions a previous run
// left on the broker never see this run's messages.
func fresh(tag string) string {
	return "live-" + tag + "-" + strings.ToLower(core.NewID())
}

// liveStreams connects to the plaintext broker under a fresh prefix.
func liveStreams(t *testing.T, tag string, opts ...streamsmqtt.Option) (streams.Streams, string) {
	t.Helper()

	addr := liveAddr(t)
	prefix := fresh(tag)
	opts = append([]streamsmqtt.Option{streamsmqtt.WithPrefix(prefix), streamsmqtt.WithSessionExpiry(5 * time.Minute)}, opts...)
	return connect(t, addr, opts...), prefix
}

// drain collects every delivery that arrives within window, acknowledging each.
func drain(t *testing.T, ch <-chan streams.Delivery, window time.Duration) []streams.Delivery {
	t.Helper()

	var got []streams.Delivery
	for {
		select {
		case d, ok := <-ch:
			if !ok {
				return got
			}
			got = append(got, d)
			if err := d.Ack(t.Context()); err != nil {
				t.Errorf("Ack: %v", err)
			}
		case <-time.After(window):
			return got
		}
	}
}

// ids is the id of each delivery, in order.
func ids(ds []streams.Delivery) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.ID
	}
	return out
}

// settle gives an acknowledgement, a packet on the wire, time to leave before
// the connection closes.
func settle() { time.Sleep(500 * time.Millisecond) }

func TestLivePublishSubscribeAndHeaders(t *testing.T) {
	addr := liveAddr(t)
	prefix := fresh("headers")
	s := connect(t, addr, streamsmqtt.WithPrefix(prefix))
	stream, m := declareWith(t, s, streams.Stream{ID: "users", Name: "users", Subjects: []string{subject, "device/+"}}.WithHeader("lang", "go"))
	_, wire := raw(t, addr, prefix+"/"+stream.ID+"/"+subject)

	ch, err := m.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	id, err := m.Publish(t.Context(), subject, event{User: "ada"}, streams.ID("chosen-id"), streams.Header("site", "plant-3"))
	if err != nil || id != "chosen-id" {
		t.Fatalf("Publish = %q, %v", id, err)
	}
	got := recv(t, ch, within)
	if got.ID != "chosen-id" || got.Subject != subject || got.Headers.Get("lang") != "go" || got.Headers.Get("site") != "plant-3" {
		t.Errorf("delivered %q on %q with %v", got.ID, got.Subject, got.Headers)
	}
	p := recv(t, wire, within)
	if p.Properties == nil || len(p.Properties.User) != 2 || p.Properties.User[0].Key != "lang" || p.Properties.User[1].Key != "site" {
		t.Errorf("user properties on the wire = %+v", p.Properties)
	}
	if p.Payload[0] != 0xB7 || p.Payload[1] != 1 {
		t.Errorf("payload is not a version-1 frame: % x", p.Payload[:2])
	}

	// A declared filter subscribes to every subject below it and reports
	// which one each message came in on.
	devices, err := m.Subscribe(t.Context(), "device/+")
	if err != nil {
		t.Fatalf("Subscribe to a filter: %v", err)
	}
	if _, err := m.Publish(t.Context(), "device/7", event{}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := recv(t, devices, within); got.Subject != "device/7" {
		t.Errorf("Subject = %q, want device/7", got.Subject)
	}
}

// An undurable subscription acknowledges on receipt. Were it not to, the broker
// would stop sending once max_inflight_messages (1000 here) were outstanding,
// and the subscription would stall, as it once did.
func TestLiveAnUndurableSubscriptionKeepsUpPastTheInflightWindow(t *testing.T) {
	s, _ := liveStreams(t, "window")
	_, m := declare(t, s, subject)
	b, _ := streams.AsBatch(m)
	ch, err := m.Subscribe(t.Context(), subject, streams.Prefetch(256))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	const count = 2500
	values := make([]any, count)
	for i := range values {
		values[i] = event{User: fmt.Sprint(i)}
	}
	sent, err := b.PublishBatch(t.Context(), subject, values)
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	for i, want := range sent {
		if got := recv(t, ch, within); got.ID != want {
			t.Fatalf("message %d is %q, want %q: a pipelined batch keeps its order", i, got.ID, want)
		}
	}
}

// Publishes on a topic keep their order over several publishing connections,
// and other topics, published concurrently, arrive whole.
func TestLivePublishConnectionsKeepEachTopicInOrder(t *testing.T) {
	s, _ := liveStreams(t, "pool", streamsmqtt.WithPublishConnections(4))
	_, m := declare(t, s, "plant/+")
	b, _ := streams.AsBatch(m)
	ch, err := m.Subscribe(t.Context(), "plant/+", streams.Prefetch(1024))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	const topics, each = 8, 50
	done := make(chan error, topics)
	for n := range topics {
		go func() {
			values := make([]any, each)
			for i := range values {
				values[i] = event{User: fmt.Sprint(i)}
			}
			_, err := b.PublishBatch(t.Context(), fmt.Sprintf("plant/%d", n), values)
			done <- err
		}()
	}
	for range topics {
		if err := <-done; err != nil {
			t.Fatalf("PublishBatch: %v", err)
		}
	}
	next := map[string]int{}
	for range topics * each {
		got := recv(t, ch, within)
		var e event
		if err := got.Decode(&e); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if e.User != fmt.Sprint(next[got.Subject]) {
			t.Fatalf("%s delivered %s, want %d", got.Subject, e.User, next[got.Subject])
		}
		next[got.Subject]++
	}
}

// Ack settles; nak leaves the message with the session, and it comes back when
// the consumer does. A session kept for another stream under the same consumer
// and subject is subscribed again and hears the new stream.
func TestLiveDurableAckNakAndRedelivery(t *testing.T) {
	s, _ := liveStreams(t, "durable")
	_, m := declare(t, s, subject)
	d := mustDurable(t, m)
	consumer := fresh("billing")
	prefetch := streams.Prefetch(1)

	ctx, cancel := context.WithCancel(t.Context())
	ch, err := d.Consume(ctx, subject, consumer, prefetch)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	acked, _ := m.Publish(t.Context(), subject, event{User: "acked"})
	naked, _ := m.Publish(t.Context(), subject, event{User: "naked"})
	first := recv(t, ch, within)
	if first.ID != acked || first.Attempt != 0 {
		t.Errorf("first = %q attempt %d, want %q attempt 0", first.ID, first.Attempt, acked)
	}
	_ = first.Ack(ctx)
	second := recv(t, ch, within)
	if second.ID != naked {
		t.Errorf("second = %q, want %q", second.ID, naked)
	}
	_ = second.Nak(ctx)
	settle()
	cancel()
	settle()

	away, _ := m.Publish(t.Context(), subject, event{User: "away"})
	again, err := d.Consume(t.Context(), subject, consumer, prefetch)
	if err != nil {
		t.Fatalf("Consume (again): %v", err)
	}
	if got := ids(drain(t, again, 3*time.Second)); fmt.Sprint(got) != fmt.Sprint([]string{naked, away}) {
		t.Errorf("resumed with %v, want the nak'd %s then the queued %s, never the acked one", got, naked, away)
	}
}

func TestLiveAResumedSessionHearsANewStream(t *testing.T) {
	s, _ := liveStreams(t, "moved")
	_, first := declare(t, s, subject)
	_, second := declare(t, s, subject)
	consumer := fresh("ledger")

	ctx, cancel := context.WithCancel(t.Context())
	if _, err := mustDurable(t, first).Consume(ctx, subject, consumer); err != nil {
		t.Fatalf("Consume (first stream): %v", err)
	}
	settle()
	cancel()
	settle()

	moved, err := mustDurable(t, second).Consume(t.Context(), subject, consumer)
	if err != nil {
		t.Fatalf("Consume (second stream): %v", err)
	}
	id, _ := second.Publish(t.Context(), subject, event{User: "moved"})
	if got := ids(drain(t, moved, 3*time.Second)); len(got) != 1 || got[0] != id {
		t.Errorf("resumed session heard %v, want [%s]", got, id)
	}
}

// A group is a shared subscription: each message goes to exactly one member.
func TestLiveGroupsShareTheSubject(t *testing.T) {
	s, _ := liveStreams(t, "group")
	_, m := declare(t, s, subject)
	d := mustDurable(t, m)
	const total = 8

	group := streams.Group(fresh("workers"))
	a, err := d.Consume(t.Context(), subject, "worker", group)
	if err != nil {
		t.Fatalf("Consume (a): %v", err)
	}
	b, err := d.Consume(t.Context(), subject, "worker", group)
	if err != nil {
		t.Fatalf("Consume (b): %v", err)
	}
	sent := map[string]bool{}
	for i := range total {
		id, _ := m.Publish(t.Context(), subject, event{Action: fmt.Sprint(i)})
		sent[id] = true
	}
	for range total {
		var got streams.Delivery
		select {
		case got = <-a:
		case got = <-b:
		case <-time.After(within):
			t.Fatal("the group stopped short")
		}
		if !sent[got.ID] {
			t.Errorf("%q arrived twice, or was never sent", got.ID)
		}
		delete(sent, got.ID)
		_ = got.Ack(t.Context())
	}
	quiet(t, a, 500*time.Millisecond, "a durable member, after the subject was shared out")
	quiet(t, b, 500*time.Millisecond, "a durable member, after the subject was shared out")

	readers := streams.Group(fresh("readers"))
	ra, _ := m.Subscribe(t.Context(), subject, readers)
	rb, _ := m.Subscribe(t.Context(), subject, readers)
	for i := range total {
		id, _ := m.Publish(t.Context(), subject, event{Action: fmt.Sprint(i)})
		sent[id] = true
	}
	for range total {
		var got streams.Message
		select {
		case got = <-ra:
		case got = <-rb:
		case <-time.After(within):
			t.Fatal("the readers stopped short")
		}
		if !sent[got.ID] {
			t.Errorf("%q arrived twice, or was never sent", got.ID)
		}
		delete(sent, got.ID)
	}
	quiet(t, ra, 500*time.Millisecond, "a reader, after the subject was shared out")
	quiet(t, rb, 500*time.Millisecond, "a reader, after the subject was shared out")
}

func TestLiveExpiryDropsWhatWaitedTooLong(t *testing.T) {
	s, _ := liveStreams(t, "expiry")
	_, m := declare(t, s, subject)
	d := mustDurable(t, m)
	consumer := fresh("exp")

	// The session exists, and nobody is attached to it.
	ctx, cancel := context.WithCancel(t.Context())
	if _, err := d.Consume(ctx, subject, consumer); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	settle()
	cancel()
	settle()

	// Sub-second: rounded up to one second.
	short, _ := m.Publish(t.Context(), subject, event{User: "short"}, streams.Expires(400*time.Millisecond))
	long, _ := m.Publish(t.Context(), subject, event{User: "long"}, streams.Expires(2*time.Minute))
	forever, _ := m.Publish(t.Context(), subject, event{User: "forever"})
	// A retained value with an expiry is gone for later subscribers too.
	if _, err := m.Publish(t.Context(), subject, event{User: "retained"}, streams.Retain(), streams.Expires(time.Second)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	time.Sleep(2500 * time.Millisecond)

	resumed, err := d.Consume(t.Context(), subject, consumer)
	if err != nil {
		t.Fatalf("Consume (resumed): %v", err)
	}
	got := ids(drain(t, resumed, 2*time.Second))
	for _, id := range got {
		if id == short {
			t.Error("the expired message was delivered")
		}
	}
	if len(got) < 2 || got[0] != long || got[1] != forever {
		t.Errorf("resumed with %v, want %s then %s", got, long, forever)
	}
	late, err := m.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	quiet(t, late, time.Second, "a later subscriber, of an expired retained value")
}

func TestLiveRetainReachesLaterSubscribersOnceAndClears(t *testing.T) {
	s, prefix := liveStreams(t, "retain")
	stream, m := declareWith(t, s, streams.Stream{ID: "r", Name: "r", Subjects: []string{subject}})
	d := mustDurable(t, m)
	sent, err := m.Publish(t.Context(), subject, event{User: "last"}, streams.Retain(), streams.Header("lang", "go"))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Another process joins later and is handed it, framed and decoded.
	other := connect(t, liveAddr(t), streamsmqtt.WithPrefix(prefix))
	_, late := declareWith(t, other, streams.Stream{ID: "r", Name: "r", Subjects: []string{subject}})
	sub, err := late.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	got := recv(t, sub, within)
	var e event
	if derr := got.Decode(&e); derr != nil || got.ID != sent || e.User != "last" || got.Headers.Get("lang") != "go" {
		t.Errorf("retained %q %+v %v (%v)", got.ID, e, got.Headers, derr)
	}

	// A durable consumer gets it when its subscription is new, and not again
	// when it resumes.
	consumer := fresh("ret")
	ctx, cancel := context.WithCancel(t.Context())
	first, err := d.Consume(ctx, subject, consumer)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if got := recv(t, first, within); got.ID != sent {
		t.Errorf("durable consumer handed %q, want %q", got.ID, sent)
	} else {
		_ = got.Ack(ctx)
	}
	settle()
	cancel()
	settle()
	resumed, err := d.Consume(t.Context(), subject, consumer)
	if err != nil {
		t.Fatalf("Consume (resumed): %v", err)
	}
	if got := drain(t, resumed, time.Second); len(got) != 0 {
		t.Errorf("the retained value came back on resume: %v", ids(got))
	}

	// Cleared: the live subscriber skips the empty message, and the next
	// subscriber is handed nothing.
	if cerr := streamsmqtt.ClearRetained(t.Context(), s, stream.ID, subject); cerr != nil {
		t.Fatalf("ClearRetained: %v", cerr)
	}
	quiet(t, sub, 500*time.Millisecond, "a live subscriber, of the clear")
	later, err := late.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	quiet(t, later, time.Second, "a subscriber after the clear")
}
