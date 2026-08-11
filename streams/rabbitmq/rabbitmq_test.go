package rabbitmq_test

// These need a live broker and skip without one. RabbitMQ has no embeddable Go
// server, unlike the Kafka, NATS and MQTT providers, so this is the one suite
// in the module that cannot run entirely in-process:
//
//	docker compose -f ../docker/compose.yaml up -d
//	go test ./...

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/streams"
	streamsrabbit "github.com/the-protobuf-project/runtime-go/streams/rabbitmq"
)

type event struct {
	User   string `json:"user"`
	Action string `json:"action"`
}

const subject = "user.created"

var seq atomic.Int64

func testURL() string {
	if url := os.Getenv("RABBITMQ_TEST_URL"); url != "" {
		return url
	}
	host, port := os.Getenv("RABBITMQ_TEST_HOST"), os.Getenv("RABBITMQ_TEST_PORT")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "5672"
	}
	return "amqp://guest:guest@" + net.JoinHostPort(host, port) + "/"
}

// prefix keeps one test's exchanges and queues away from another's.
func prefix() string {
	return fmt.Sprintf("t%d-%d", time.Now().UnixNano(), seq.Add(1))
}

func testStreams(t *testing.T, opts ...streamsrabbit.Option) streams.Streams {
	t.Helper()

	// Fail fast when nothing is listening, rather than waiting out the AMQP
	// handshake timeout on every test in the file.
	d := net.Dialer{Timeout: 2 * time.Second}
	conn, err := d.DialContext(t.Context(), "tcp", hostPort())
	if err != nil {
		t.Skipf("no RabbitMQ at %s (%v); skipping", hostPort(), err)
	}
	_ = conn.Close()

	opts = append([]streamsrabbit.Option{streamsrabbit.WithPrefix(prefix())}, opts...)
	s, err := streamsrabbit.Connect(t.Context(), testURL(), opts...)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = s.(streams.Closer).Close() })
	return s
}

func hostPort() string {
	host, port := os.Getenv("RABBITMQ_TEST_HOST"), os.Getenv("RABBITMQ_TEST_PORT")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "5672"
	}
	return net.JoinHostPort(host, port)
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

// A stream may declare a binding pattern; a message still needs a concrete key.
func TestWildcardSubjects(t *testing.T) {
	_, m := declare(t, testStreams(t), "user.*")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := m.Subscribe(ctx, "user.*")
	if err != nil {
		t.Fatalf("Subscribe to the pattern: %v", err)
	}
	if _, perr := m.Publish(ctx, "user.created", event{User: "ada"}); perr != nil {
		t.Fatalf("Publish to a subject the pattern covers: %v", perr)
	}

	select {
	case msg := <-ch:
		if msg.Subject != "user.created" {
			t.Errorf("Subject = %q, want the concrete routing key", msg.Subject)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("nothing was delivered")
	}

	if _, perr := m.Publish(ctx, "user.*", event{}); !errors.Is(perr, streams.ErrUnknownSubject) {
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

func TestConsumeRejectsAGroupOption(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	_, err := d.Consume(t.Context(), subject, "billing", streams.Group("other"))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume with a Group = %v, want ErrUnsupported", err)
	}
}

func TestConsumeRequiresAConsumerName(t *testing.T) {
	_, m := declare(t, testStreams(t), subject)
	d, _ := streams.AsDurable(m)

	if _, err := d.Consume(t.Context(), subject, ""); !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Consume without a name = %v, want ErrUnsupported", err)
	}
}

func TestConnectRejectsAnEmptyURL(t *testing.T) {
	if _, err := streamsrabbit.Connect(t.Context(), ""); err == nil {
		t.Error("Connect accepted an empty URL")
	}
}
