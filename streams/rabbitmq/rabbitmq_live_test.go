//go:build live

package rabbitmq_test

// The provider against the plaintext Docker broker, ported from runtime-rs's
// tests/rabbitmq_live.rs. They skip when nothing is listening:
//
//	docker compose -p tpp-streams-live-rabbitmq -f ../docker/compose.rabbitmq.yaml up -d --wait
//	go test -race -tags live ./rabbitmq/... -run Live
//
// RABBITMQ_TEST_URL, RABBITMQ_TEST_MANAGEMENT and RABBITMQ_TEST_CONTAINER
// point them elsewhere. Each test declares its streams under a prefix of its
// own, so tests never see each other's exchanges or queues.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
	streamsrabbit "github.com/the-protobuf-project/runtime-go/streams/rabbitmq"
)

// liveWait is how long a test waits for a delivery before calling it lost.
const liveWait = 10 * time.Second

// liveURL is the plaintext broker: RABBITMQ_TEST_URL or the compose default.
func liveURL() string {
	if u := os.Getenv("RABBITMQ_TEST_URL"); u != "" {
		return u
	}
	return "amqp://guest:guest@127.0.0.1:15682/"
}

// liveManagement is the plaintext broker's management API.
func liveManagement() string {
	if u := os.Getenv("RABBITMQ_TEST_MANAGEMENT"); u != "" {
		return u
	}
	return "http://127.0.0.1:15683"
}

// liveContainer is the plaintext broker's container, for its own tools.
func liveContainer() string {
	if c := os.Getenv("RABBITMQ_TEST_CONTAINER"); c != "" {
		return c
	}
	return "tpp-streams-rabbitmq"
}

// live is one test's provider, its prefix, and a raw connection of its own
// for reading the broker as another client would.
type live struct {
	s      streams.Streams
	prefix string
	raw    *amqp.Connection
}

// liveStreams connects to the plaintext broker under a fresh prefix, skipping
// when nothing is listening.
func liveStreams(t *testing.T, opts ...streamsrabbit.Option) live {
	t.Helper()

	uri, err := amqp.ParseURI(liveURL())
	if err != nil {
		t.Fatalf("RabbitMQ live URL: %v", err)
	}
	addr := net.JoinHostPort(uri.Host, strconv.Itoa(uri.Port))
	d := net.Dialer{Timeout: 2 * time.Second}
	conn, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Skipf("no RabbitMQ at %s (%v); see docker/compose.rabbitmq.yaml", addr, err)
	}
	_ = conn.Close()

	p := prefix()
	opts = append([]streamsrabbit.Option{streamsrabbit.WithPrefix(p)}, opts...)
	s, err := streamsrabbit.Connect(t.Context(), liveURL(), opts...)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.(streams.Closer).Close() })
	raw, err := amqp.Dial(liveURL())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	return live{s: s, prefix: p, raw: raw}
}

// headed declares a stream over [subject] with h as its defaults and binds it.
func (l live) headed(t *testing.T, h streams.Headers) (streams.Stream, streams.Manager) {
	t.Helper()
	stream, err := l.s.Create(t.Context(), streams.Stream{Name: "headers", Subjects: []string{subject}}.WithHeaders(h))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := l.s.Bind(t.Context(), stream.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return stream, m
}

// exchange is the broker's name for stream.
func (l live) exchange(stream streams.Stream) string { return l.prefix + "." + stream.ID }

// queue is the durable queue consumer reads [subject] from on stream.
func (l live) queue(stream streams.Stream, consumer string) string {
	return l.exchange(stream) + "." + consumer + ".user-created"
}

// park declares consumer's queue and detaches, so publishes wait in it.
func park(t *testing.T, m streams.Manager, consumer string) {
	t.Helper()
	d, _ := streams.AsDurable(m)
	ctx, cancel := context.WithCancel(t.Context())
	if _, err := d.Consume(ctx, subject, consumer); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	cancel()
	time.Sleep(300 * time.Millisecond)
}

// peek is the broker's own view of queue's messages through the management
// API, leaving them in the queue.
func peek(t *testing.T, queue string) []map[string]any {
	t.Helper()
	body := `{"count":10,"ackmode":"ack_requeue_true","encoding":"base64"}`
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		liveManagement()+"/api/queues/%2F/"+url.PathEscape(queue)+"/get", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("guest", "guest")
	req.Header.Set("content-type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Skipf("no RabbitMQ management API at %s (%v)", liveManagement(), err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("management API get %s: %s", queue, res.Status)
	}
	var out []map[string]any
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

// depth is how many messages the broker holds in queue, delivered or not,
// from rabbitmqctl inside the container, which unlike the management API's
// figures is not sampled.
func depth(t *testing.T, queue string) int {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "docker", "exec", liveContainer(),
		"rabbitmqctl", "-q", "list_queues", "name", "messages").Output()
	if err != nil {
		t.Fatalf("rabbitmqctl list_queues: %v", err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == queue {
			n, _ := strconv.Atoi(fields[1])
			return n
		}
	}
	t.Fatalf("no queue %s", queue)
	return 0
}

// quietly asserts nothing arrives on ch within d.
func quietly[T any](t *testing.T, ch <-chan T, d time.Duration, what string) {
	t.Helper()
	select {
	case v, ok := <-ch:
		if ok {
			t.Fatalf("%s: got %+v", what, v)
		}
	case <-time.After(d):
	}
}

func TestLivePublishKeepsAChosenID(t *testing.T) {
	l := liveStreams(t)
	_, m := l.headed(t, nil)
	sub, err := m.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	id, err := m.Publish(t.Context(), subject, event{User: "ada"}, streams.ID("mine-1"))
	if err != nil || id != "mine-1" {
		t.Fatalf("Publish = %q, %v", id, err)
	}
	if got := recv(t, sub, liveWait); got.ID != "mine-1" {
		t.Errorf("delivered %q", got.ID)
	}
}

// A durable queue and a batch, but not a log.
func TestLiveDurableAndBatchButNotPositioned(t *testing.T) {
	_, m := liveStreams(t).headed(t, nil)
	if _, err := streams.AsDurable(m); err != nil {
		t.Errorf("AsDurable: %v", err)
	}
	if _, err := streams.AsBatch(m); err != nil {
		t.Errorf("AsBatch: %v", err)
	}
	if _, err := streams.AsPositioned(m); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("AsPositioned = %v, want ErrUnsupported", err)
	}
}

// Ending a consumer with a delivery unsettled returns the message to the queue.
func TestLiveDroppingTheConsumerReturnsUnsettledMessages(t *testing.T) {
	_, m := liveStreams(t).headed(t, nil)
	d, _ := streams.AsDurable(m)

	ctx, cancel := context.WithCancel(t.Context())
	first, err := d.Consume(ctx, subject, "billing")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, xerr := m.Publish(t.Context(), subject, event{User: "ada"}); xerr != nil {
		t.Fatalf("Publish: %v", xerr)
	}
	id := recvDelivery(t, first, liveWait).ID
	cancel()
	time.Sleep(500 * time.Millisecond)

	again, err := d.Consume(t.Context(), subject, "billing")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	back := recvDelivery(t, again, liveWait)
	if back.ID != id || back.Attempt < 2 {
		t.Errorf("got %q attempt %d, want %q again", back.ID, back.Attempt, id)
	}
	_ = back.Ack(t.Context())
}

// With a subscription's auto-delete queue gone the exchange routes to nobody,
// and a publish is still accepted: AMQP drops unroutable messages by default.
func TestLiveDroppingASubscriptionReleasesItsQueue(t *testing.T) {
	l := liveStreams(t)
	_, m := l.headed(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	if _, err := m.Subscribe(ctx, subject); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cancel()
	time.Sleep(500 * time.Millisecond)
	if _, err := m.Publish(t.Context(), subject, event{User: "ada"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

// A batch arrives in order, and a chosen id is refused for it.
func TestLiveBatchPublishesInOrder(t *testing.T) {
	_, m := liveStreams(t).headed(t, nil)
	sub, err := m.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	b, _ := streams.AsBatch(m)
	ids, err := b.PublishBatch(t.Context(), subject, []any{event{User: "a"}, event{User: "b"}, event{User: "c"}})
	if err != nil || len(ids) != 3 {
		t.Fatalf("PublishBatch = %v, %v", ids, err)
	}
	for _, id := range ids {
		if got := recv(t, sub, liveWait); got.ID != id {
			t.Errorf("delivered %q, want %q", got.ID, id)
		}
	}
	if _, err := b.PublishBatch(t.Context(), subject, []any{event{}}, streams.ID("x")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("PublishBatch with an id = %v, want ErrUnsupported", err)
	}
}

// Options that change what a call means are refused by name.
func TestLiveRefusals(t *testing.T) {
	_, m := liveStreams(t).headed(t, nil)
	d, _ := streams.AsDurable(m)
	ctx := t.Context()

	if _, err := m.Publish(ctx, "typo", event{}); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Publish(typo) = %v", err)
	}
	if _, err := m.Subscribe(ctx, "typo"); !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Subscribe(typo) = %v", err)
	}
	if _, err := m.Publish(ctx, subject, event{}, streams.TTL(time.Second)); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish with a TTL = %v", err)
	}
	if _, err := m.Subscribe(ctx, subject, streams.Group("g")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Subscribe with a Group = %v", err)
	}
	if _, err := d.Consume(ctx, subject, "billing", streams.Group("other")); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume with a Group = %v", err)
	}
	if _, err := d.Consume(ctx, subject, ""); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume without a name = %v", err)
	}
}

// A call's Prefetch is the consumer's basic.qos: with 1, the second message
// waits until the first is settled.
func TestLivePrefetchOneHoldsTheNext(t *testing.T) {
	_, m := liveStreams(t).headed(t, nil)
	d, _ := streams.AsDurable(m)
	ch, err := d.Consume(t.Context(), subject, "billing", streams.Prefetch(1))
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	first, _ := m.Publish(t.Context(), subject, event{User: "one"})
	second, _ := m.Publish(t.Context(), subject, event{User: "two"})
	got := recvDelivery(t, ch, liveWait)
	if got.ID != first {
		t.Fatalf("delivered %q, want %q", got.ID, first)
	}
	quietly(t, ch, 700*time.Millisecond, "a second message arrived past prefetch 1")
	_ = got.Ack(t.Context())
	if got := recvDelivery(t, ch, liveWait); got.ID != second {
		t.Errorf("delivered %q, want %q", got.ID, second)
	}
}

// Stream defaults arrive and a publish's own win, on single publishes over
// every channel of the pool and on batches.
func TestLiveHeadersTravelInTheAMQPProperties(t *testing.T) {
	_, m := liveStreams(t).headed(t, streams.Headers{"lang": "go", "site": "plant-3"})
	sub, err := m.Subscribe(t.Context(), subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, xerr := m.Publish(t.Context(), subject, event{User: "ada"}); xerr != nil {
		t.Fatalf("Publish: %v", xerr)
	}
	got := recv(t, sub, liveWait)
	if want := (streams.Headers{"lang": "go", "site": "plant-3"}); !reflect.DeepEqual(got.Headers, want) {
		t.Errorf("headers %v, want %v", got.Headers, want)
	}
	var ev event
	if xerr := got.Decode(&ev); xerr != nil || ev.User != "ada" {
		t.Errorf("Decode = %+v, %v", ev, xerr)
	}

	// More publishes than channels, so every channel carries them.
	want := streams.Headers{"lang": "go", "site": "plant-4", "traceparent": "00-ab"}
	for range 8 {
		if _, xerr := m.Publish(t.Context(), subject, event{User: "grace"},
			streams.Header("site", "plant-4"), streams.Header("traceparent", "00-ab")); xerr != nil {
			t.Fatalf("Publish: %v", xerr)
		}
		if got := recv(t, sub, liveWait); !reflect.DeepEqual(got.Headers, want) {
			t.Errorf("headers %v, want %v", got.Headers, want)
		}
	}

	b, _ := streams.AsBatch(m)
	ids, err := b.PublishBatch(t.Context(), subject, []any{event{User: "a"}, event{User: "b"}}, streams.Header("batch", "1"))
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	want = streams.Headers{"batch": "1", "lang": "go", "site": "plant-3"}
	for _, id := range ids {
		got := recv(t, sub, liveWait)
		if got.ID != id || !reflect.DeepEqual(got.Headers, want) {
			t.Errorf("got %q %v, want %q %v", got.ID, got.Headers, id, want)
		}
	}
}

// Another tool reading the broker sees the headers as an AMQP table and the
// expiry as the expiration property; the body is the version-1 frame with no
// header in it, and a header-less publish sends no table.
func TestLiveHeadersAreNativeAMQPProperties(t *testing.T) {
	l := liveStreams(t)
	stream, m := l.headed(t, streams.Headers{"lang": "go"})
	park(t, m, "audit")
	queue := l.queue(stream, "audit")

	value := event{User: "ada", Action: "created"}
	id, err := m.Publish(t.Context(), subject, value, streams.Header("traceparent", "00-ab"), streams.Expires(time.Minute))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	seen := peek(t, queue)
	if len(seen) != 1 {
		t.Fatalf("peeked %d messages: %v", len(seen), seen)
	}
	props, _ := seen[0]["properties"].(map[string]any)
	headers, _ := props["headers"].(map[string]any)
	if headers["lang"] != "go" || headers["traceparent"] != "00-ab" || props["expiration"] != "60000" ||
		props["message_id"] != id || props["delivery_mode"] != float64(2) || props["content_type"] != "application/json" {
		t.Errorf("management API sees %v", props)
	}
	want, err := core.Pack(streams.JSON, id, value)
	if err != nil {
		t.Fatal(err)
	}
	if payload, _ := base64.StdEncoding.DecodeString(seen[0]["payload"].(string)); !bytes.Equal(payload, want) {
		t.Errorf("body %q, want the version-1 frame %q", payload, want)
	}

	// Without headers or expiry, no table and no expiration.
	plain, pm := l.headed(t, nil)
	park(t, pm, "audit")
	if _, err := pm.Publish(t.Context(), subject, event{User: "x"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	props, _ = peek(t, l.queue(plain, "audit"))[0]["properties"].(map[string]any)
	if _, ok := props["headers"]; ok {
		t.Errorf("a header-less publish sent a table: %v", props)
	}
	if _, ok := props["expiration"]; ok {
		t.Errorf("a publish without expiry sent one: %v", props)
	}
}

// A frame another client publishes with no headers table reads back with no
// headers; one carrying the broker's bookkeeping and a non-string value keeps
// only the string headers.
func TestLiveForeignMessagesReadBack(t *testing.T) {
	l := liveStreams(t)
	stream, m := l.headed(t, nil)
	d, _ := streams.AsDurable(m)
	deliveries, err := d.Consume(t.Context(), subject, "foreign")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	ch, err := l.raw.Channel()
	if err != nil {
		t.Fatal(err)
	}
	send := func(id string, headers amqp.Table) {
		body, err := core.Pack(streams.JSON, id, event{User: "rust"})
		if err != nil {
			t.Fatal(err)
		}
		if err := ch.PublishWithContext(t.Context(), l.exchange(stream), subject, false, false, amqp.Publishing{
			MessageId: id, DeliveryMode: amqp.Persistent, ContentType: "application/json", Headers: headers, Body: body,
		}); err != nil {
			t.Fatal(err)
		}
	}

	send("rs-1", nil)
	got := recvDelivery(t, deliveries, liveWait)
	var ev event
	if got.ID != "rs-1" || got.Headers != nil || got.Decode(&ev) != nil || ev.User != "rust" {
		t.Errorf("got %q %v %+v", got.ID, got.Headers, ev)
	}
	_ = got.Ack(t.Context())

	send("rs-2", amqp.Table{"lang": "rust", "count": int32(3), "x-origin": "shovel"})
	got = recvDelivery(t, deliveries, liveWait)
	if want := (streams.Headers{"lang": "rust"}); !reflect.DeepEqual(got.Headers, want) {
		t.Errorf("headers %v, want %v", got.Headers, want)
	}
	_ = got.Ack(t.Context())
}

// Headers come back with a redelivery after a nak and after the consumer
// ended with the message unsettled.
func TestLiveHeadersSurviveRedelivery(t *testing.T) {
	_, m := liveStreams(t).headed(t, streams.Headers{"lang": "go"})
	d, _ := streams.AsDurable(m)
	ctx, cancel := context.WithCancel(t.Context())
	ch, err := d.Consume(ctx, subject, "billing")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, xerr := m.Publish(t.Context(), subject, event{User: "ada"}, streams.Header("try", "1")); xerr != nil {
		t.Fatalf("Publish: %v", xerr)
	}
	want := streams.Headers{"lang": "go", "try": "1"}
	first := recvDelivery(t, ch, liveWait)
	if !reflect.DeepEqual(first.Headers, want) {
		t.Errorf("headers %v, want %v", first.Headers, want)
	}
	if xerr := first.Nak(t.Context()); xerr != nil {
		t.Fatalf("Nak: %v", xerr)
	}
	again := recvDelivery(t, ch, liveWait)
	if again.Attempt < 2 || !reflect.DeepEqual(again.Headers, want) {
		t.Errorf("after nak: attempt %d, headers %v", again.Attempt, again.Headers)
	}
	cancel()
	time.Sleep(300 * time.Millisecond)

	ch, err = d.Consume(t.Context(), subject, "billing")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	back := recvDelivery(t, ch, liveWait)
	if !reflect.DeepEqual(back.Headers, want) {
		t.Errorf("after the consumer ended: headers %v, want %v", back.Headers, want)
	}
	_ = back.Ack(t.Context())
}

// Invalid and RabbitMQ-reserved headers are refused at Create, Update and
// publish, before anything reaches the broker.
func TestLiveBadHeadersAreRefused(t *testing.T) {
	l := liveStreams(t)
	ctx := t.Context()
	long := strings.Repeat("k", 256)
	bad := []struct{ key, value string }{
		{"x-death", "v"}, {"x-delivery-count", "1"}, {"CC", "user.deleted"}, {"BCC", "user.deleted"},
		{"has space", "v"}, {"lang", "two\nlines"}, {long, "v"},
	}
	for _, h := range bad {
		in := streams.Stream{Name: "bad", Subjects: []string{subject}}.WithHeader(h.key, h.value)
		if _, err := l.s.Create(ctx, in); !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("Create with %.20q = %v, want ErrInvalidHeader", h.key, err)
		}
	}
	if list, _ := l.s.List(ctx); len(list) != 0 {
		t.Errorf("a refused Create stored a stream: %v", list)
	}

	good, m := l.headed(t, streams.Headers{"lang": "go"})
	in := streams.Stream{Name: "ok", Subjects: []string{subject}}.WithHeader("x-a", "b")
	if _, err := l.s.Update(ctx, good.ID, in); !errors.Is(err, streams.ErrInvalidHeader) {
		t.Errorf("Update with x-a = %v, want ErrInvalidHeader", err)
	}
	if got, _ := l.s.Get(ctx, good.ID); got.Headers.Get("lang") != "go" {
		t.Errorf("a refused Update changed the stream: %v", got.Headers)
	}

	b, _ := streams.AsBatch(m)
	for _, h := range bad {
		if _, err := m.Publish(ctx, subject, event{}, streams.Header(h.key, h.value)); !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("Publish with %.20q = %v, want ErrInvalidHeader", h.key, err)
		}
		if _, err := b.PublishBatch(ctx, subject, []any{event{}}, streams.Header(h.key, h.value)); !errors.Is(err, streams.ErrInvalidHeader) {
			t.Errorf("PublishBatch with %.20q = %v, want ErrInvalidHeader", h.key, err)
		}
	}
}

// An expired message is dropped by the broker, not delivered; one that has
// not expired is delivered.
func TestLiveExpiredMessagesAreNotDelivered(t *testing.T) {
	_, m := liveStreams(t).headed(t, nil)
	park(t, m, "late")
	if _, err := m.Publish(t.Context(), subject, event{User: "stale"}, streams.Expires(200*time.Millisecond)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	time.Sleep(800 * time.Millisecond)
	fresh, err := m.Publish(t.Context(), subject, event{User: "fresh"}, streams.Expires(time.Minute))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d, _ := streams.AsDurable(m)
	ch, err := d.Consume(t.Context(), subject, "late")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	got := recvDelivery(t, ch, liveWait)
	if got.ID != fresh {
		t.Errorf("delivered %q, want %q", got.ID, fresh)
	}
	_ = got.Ack(t.Context())
	quietly(t, ch, 500*time.Millisecond, "an expired message was delivered")

	// An expiry RabbitMQ cannot hold is refused by name, and zero means none.
	tooLong := (math.MaxUint32 + 1) * time.Millisecond
	if _, xerr := m.Publish(t.Context(), subject, event{}, streams.Expires(tooLong)); !errors.Is(xerr, streams.ErrUnsupported) {
		t.Errorf("Publish with an expiry past the limit = %v, want ErrUnsupported", xerr)
	}
	none, err := m.Publish(t.Context(), subject, event{User: "zero"}, streams.Expires(0))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := recvDelivery(t, ch, liveWait); got.ID != none {
		t.Errorf("delivered %q, want %q", got.ID, none)
	}
}

// RabbitMQ expires a message only once it reaches the head of its queue: a
// short-lived message behind a long-lived one stays in the queue past its
// expiry, and is dropped, not delivered, once it gets there.
func TestLiveExpiryWaitsForTheHeadOfTheQueue(t *testing.T) {
	l := liveStreams(t)
	stream, m := l.headed(t, nil)
	park(t, m, "slow")
	queue := l.queue(stream, "slow")
	long, err := m.Publish(t.Context(), subject, event{User: "long"}, streams.Expires(time.Minute))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, xerr := m.Publish(t.Context(), subject, event{User: "short"}, streams.Expires(200*time.Millisecond)); xerr != nil {
		t.Fatalf("Publish: %v", xerr)
	}
	time.Sleep(800 * time.Millisecond)
	if n := depth(t, queue); n != 2 {
		t.Errorf("depth %d, want 2: the expired message is still held behind the head", n)
	}

	d, _ := streams.AsDurable(m)
	ch, err := d.Consume(t.Context(), subject, "slow")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	got := recvDelivery(t, ch, liveWait)
	if got.ID != long {
		t.Errorf("delivered %q, want %q", got.ID, long)
	}
	_ = got.Ack(t.Context())
	quietly(t, ch, 500*time.Millisecond, "the expired message was delivered once it reached the head")
	if n := depth(t, queue); n != 0 {
		t.Errorf("depth %d after the head was consumed, want 0", n)
	}
}

// Retain and a delayed nak are refused by name; a refused delayed nak still
// hands the message straight back rather than stranding it.
func TestLiveRetainAndDelayedNakAreRefused(t *testing.T) {
	_, m := liveStreams(t).headed(t, nil)
	_, err := m.Publish(t.Context(), subject, event{}, streams.Retain())
	if !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), "last value") {
		t.Errorf("Publish with Retain = %v, want ErrUnsupported naming the last value", err)
	}
	b, _ := streams.AsBatch(m)
	if _, xerr := b.PublishBatch(t.Context(), subject, []any{event{}}, streams.Retain()); !errors.Is(xerr, streams.ErrUnsupported) {
		t.Errorf("PublishBatch with Retain = %v, want ErrUnsupported", xerr)
	}

	d, _ := streams.AsDurable(m)
	ch, err := d.Consume(t.Context(), subject, "retry")
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	id, err := m.Publish(t.Context(), subject, event{User: "ada"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	first := recvDelivery(t, ch, liveWait)
	if first.CanDelayRedelivery() || first.MaxRedeliveryDelay() != 0 {
		t.Error("RabbitMQ claims it can hold a redelivery back")
	}
	if err := first.NakAfter(t.Context(), 30*time.Second); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("NakAfter = %v, want ErrUnsupported", err)
	}
	again := recvDelivery(t, ch, 5*time.Second)
	if again.ID != id {
		t.Errorf("delivered %q, want %q handed straight back", again.ID, id)
	}
	_ = again.Ack(t.Context())
}

// Closing a store built by Use closes its publishing channels and never the
// caller's connection; a store whose given connection has gone fails fast.
func TestLiveUseCloseLeavesTheConnection(t *testing.T) {
	l := liveStreams(t)
	conn, err := amqp.Dial(liveURL())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	s := streamsrabbit.Use(conn, streamsrabbit.WithPrefix(l.prefix))
	stream, err := s.Create(t.Context(), streams.Stream{Name: "use", Subjects: []string{subject}})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, _ := s.Bind(t.Context(), stream.ID)
	if _, xerr := m.Publish(t.Context(), subject, event{}); xerr != nil {
		t.Fatalf("Publish: %v", xerr)
	}
	if xerr := s.(streams.Closer).Close(); xerr != nil {
		t.Fatalf("Close: %v", xerr)
	}
	if conn.IsClosed() {
		t.Fatal("Close closed the caller's connection")
	}
	if _, xerr := m.Publish(t.Context(), subject, event{}); xerr == nil {
		t.Error("Publish succeeded after Close")
	}
	// The caller's connection still works for the caller.
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("Channel on the caller's connection after Close: %v", err)
	}
	_ = ch.ExchangeDelete(l.exchange(stream), false, false)

	// A second store on a connection that then closes fails at once.
	other := streamsrabbit.Use(conn, streamsrabbit.WithPrefix(l.prefix))
	again, err := other.Create(t.Context(), streams.Stream{Name: "gone", Subjects: []string{subject}})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	om, _ := other.Bind(t.Context(), again.ID)
	_ = conn.Close()
	start := time.Now()
	if _, err := om.Publish(t.Context(), subject, event{}); err == nil {
		t.Error("Publish succeeded on a closed connection")
	}
	if time.Since(start) > 2*time.Second {
		t.Error("Publish waited for a connection nothing will redial")
	}
	if rc, err := l.raw.Channel(); err == nil {
		_ = rc.ExchangeDelete(l.exchange(again), false, false)
		_ = rc.Close()
	}
}
