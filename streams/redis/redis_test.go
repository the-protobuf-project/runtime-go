package redis_test

// These tests need a live server and skip without one. The scheduled-delivery
// tests additionally need `--notify-keyspace-events Ex`, which
// ../../docker/compose.yaml sets:
//
//	docker compose -f ../../docker/compose.yaml up -d
//	go test ./...

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/streams"
	streamsredis "github.com/the-protobuf-project/runtime-go/streams/redis"
)

type event struct {
	User   string `json:"user"`
	Action string `json:"action"`
}

const subject = "user.created"

var seq atomic.Int64

func testAddr() string {
	host, port := os.Getenv("REDIS_TEST_HOST"), os.Getenv("REDIS_TEST_PORT")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "6379"
	}
	return net.JoinHostPort(host, port)
}

func testClient(t *testing.T) *goredis.Client {
	t.Helper()

	rdb := goredis.NewClient(&goredis.Options{Addr: testAddr(), DB: 3})
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("no Redis at %s (%v); skipping", testAddr(), err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func prefix() string {
	return fmt.Sprintf("t%d_%d", time.Now().UnixNano(), seq.Add(1))
}

// newStreams returns immediate streams under a prefix unique to this test.
func newStreams(t *testing.T) streams.Streams {
	t.Helper()
	return streamsredis.Connect(testClient(t), streamsredis.WithPrefix(prefix()))
}

// bind creates a stream and binds a manager to it.
func bind(t *testing.T, s streams.Streams, subjects ...string) (streams.Stream, streams.Manager) {
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

func TestStreamLifecycle(t *testing.T) {
	s := newStreams(t)
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

	updated, err := s.Update(ctx, created.ID, streams.Stream{
		Name: "users", Description: "after", Subjects: []string{subject},
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.ID != created.ID {
		t.Errorf("Update changed the id: %q -> %q", created.ID, updated.ID)
	}

	// Update must leave exactly one metadata entry, not accumulate them.
	got, err := s.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Description != "after" {
		t.Errorf("Get returned stale metadata: %q", got.Description)
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

// A silent nil here once made a failed delete look like a completed one.
func TestDeleteOnAMissingStreamReportsNotFound(t *testing.T) {
	s := newStreams(t)

	if err := s.Delete(t.Context(), "absent"); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Delete error = %v, want streams.ErrNotFound", err)
	}
}

// Bind must fail on an unknown stream rather than hand back something that
// panics on first use.
func TestBindOnAMissingStreamReturnsError(t *testing.T) {
	s := newStreams(t)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Bind panicked on a missing stream: %v", r)
		}
	}()

	if _, err := s.Bind(t.Context(), "absent"); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Bind error = %v, want streams.ErrNotFound", err)
	}
}

// The caller's model round-trips, and the publisher's id survives delivery.
func TestPublishAndSubscribe(t *testing.T) {
	s := newStreams(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	_, m := bind(t, s, subject)

	msgs, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	want := event{User: "alice", Action: "created"}
	id, err := m.Publish(ctx, subject, want)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-msgs:
		if msg.ID != id {
			t.Errorf("delivered id = %q, want %q", msg.ID, id)
		}
		if msg.Subject != subject {
			t.Errorf("delivered subject = %q, want %q", msg.Subject, subject)
		}
		var got event
		if derr := msg.Decode(&got); derr != nil {
			t.Fatalf("Decode: %v", derr)
		}
		if got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the message")
	}
}

// Publish must deliver exactly once and must not block.
func TestPublishDeliversOnceWithoutBlocking(t *testing.T) {
	s := newStreams(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	_, m := bind(t, s, subject)

	msgs, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	start := time.Now()
	if _, perr := m.Publish(ctx, subject, event{User: "a"}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Publish blocked for %v; it should not sleep", elapsed)
	}

	received := 0
	timeout := time.After(time.Second)
collect:
	for {
		select {
		case _, ok := <-msgs:
			if !ok {
				break collect
			}
			received++
		case <-timeout:
			break collect
		}
	}
	if received != 1 {
		t.Errorf("received %d copies, want exactly 1", received)
	}
}

// A subject the stream does not declare must fail at the call that made it.
func TestUndeclaredSubjectIsRejected(t *testing.T) {
	s := newStreams(t)
	ctx := t.Context()

	_, m := bind(t, s, subject)

	if _, perr := m.Publish(ctx, "typo", event{}); !errors.Is(perr, streams.ErrUnknownSubject) {
		t.Errorf("Publish error = %v, want ErrUnknownSubject", perr)
	}
	if _, serr := m.Subscribe(ctx, "typo"); !errors.Is(serr, streams.ErrUnknownSubject) {
		t.Errorf("Subscribe error = %v, want ErrUnknownSubject", serr)
	}
}

// Two subjects on one stream must not cross-deliver.
func TestSubjectsAreIsolated(t *testing.T) {
	s := newStreams(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	_, m := bind(t, s, "a", "b")

	onA, err := m.Subscribe(ctx, "a")
	if err != nil {
		t.Fatalf("Subscribe(a): %v", err)
	}
	if _, perr := m.Publish(ctx, "b", event{User: "for b"}); perr != nil {
		t.Fatalf("Publish(b): %v", perr)
	}

	select {
	case msg := <-onA:
		t.Errorf("the subscriber on 'a' received a message published on 'b': %+v", msg)
	case <-time.After(300 * time.Millisecond):
		// nothing arrived, as intended
	}
}

// An immediate stream cannot honor a delay, and saying so beats publishing now
// and letting the caller believe it was scheduled. It is ErrUnsupported so the
// retry middleware gives up immediately rather than retrying a settled answer.
func TestImmediateStreamRejectsATTL(t *testing.T) {
	s := newStreams(t)
	ctx := t.Context()

	_, m := bind(t, s, subject)

	_, err := m.Publish(ctx, subject, event{}, streams.TTL(time.Second))
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish error = %v, want streams.ErrUnsupported", err)
	}
}

// Canceling the context must close the channel and release the goroutine.
func TestCancelingClosesTheSubscription(t *testing.T) {
	s := newStreams(t)
	ctx, cancel := context.WithCancel(t.Context())

	_, m := bind(t, s, subject)

	msgs, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	cancel()

	select {
	case _, ok := <-msgs:
		if ok {
			select {
			case _, ok := <-msgs:
				if ok {
					t.Error("the channel delivered again after cancellation")
				}
			case <-time.After(2 * time.Second):
				t.Error("the channel was not closed after cancellation")
			}
		}
	case <-time.After(2 * time.Second):
		t.Error("the channel was not closed after cancellation")
	}
}

// A consumer that walks away without draining must not strand the delivery
// goroutine.
func TestAbandonedSubscriberDoesNotLeak(t *testing.T) {
	s := newStreams(t)
	_, m := bind(t, s, subject)

	before := runtime.NumGoroutine()

	func() {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		if _, err := m.Subscribe(ctx, subject); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if _, err := m.Publish(ctx, subject, event{User: "ignored"}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("goroutines grew from %d to %d after an abandoned subscription",
		before, runtime.NumGoroutine())
}

// The typed view decodes deliveries into the caller's model.
func TestTypedViewDecodesDeliveries(t *testing.T) {
	s := newStreams(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	_, m := bind(t, s, subject)

	events := streams.For[event](m)

	msgs, err := events.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("typed Subscribe: %v", err)
	}
	if _, perr := events.Publish(ctx, subject, event{User: "grace", Action: "created"}); perr != nil {
		t.Fatalf("typed Publish: %v", perr)
	}

	select {
	case got := <-msgs:
		if got.User != "grace" {
			t.Errorf("User = %q, want grace", got.User)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the typed message")
	}
}

// --- scheduled delivery ---

// Delivery happens on expiry, and the payload survives the TTL key announcing
// it.
func TestScheduledDeliversOnExpiry(t *testing.T) {
	n := streamsredis.ConnectScheduled(testClient(t), streamsredis.WithPrefix(prefix()))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	_, m := bind(t, n, "reminder")

	msgs, err := m.Subscribe(ctx, "reminder")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	want := event{User: "alice", Action: "take a pill"}
	if _, perr := m.Publish(ctx, "reminder", want, streams.TTL(300*time.Millisecond)); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	select {
	case msg := <-msgs:
		var got event
		if derr := msg.Decode(&got); derr != nil {
			t.Fatalf("Decode: %v", derr)
		}
		if got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the notification never fired (does the server run with --notify-keyspace-events Ex?)")
	}
}

// A zero TTL could never fire; accepting it silently would strand the
// subscriber.
func TestScheduledRequiresATTL(t *testing.T) {
	n := streamsredis.ConnectScheduled(testClient(t), streamsredis.WithPrefix(prefix()))
	_, m := bind(t, n, "reminder")

	_, err := m.Publish(t.Context(), "reminder", event{})
	if !errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("Publish error = %v, want streams.ErrUnsupported", err)
	}
}

// Each message needs its own pending key, or two scheduled messages overwrite
// each other and only one fires.
func TestTwoScheduledMessagesBothFire(t *testing.T) {
	n := streamsredis.ConnectScheduled(testClient(t), streamsredis.WithPrefix(prefix()))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	_, m := bind(t, n, "reminder")

	msgs, err := m.Subscribe(ctx, "reminder")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	for _, who := range []string{"first", "second"} {
		if _, perr := m.Publish(ctx, "reminder", event{User: who},
			streams.TTL(300*time.Millisecond)); perr != nil {
			t.Fatalf("Publish(%s): %v", who, perr)
		}
	}

	seen := map[string]bool{}
	deadline := time.After(5 * time.Second)
	for len(seen) < 2 {
		select {
		case msg, ok := <-msgs:
			if !ok {
				t.Fatal("the channel closed early")
			}
			var got event
			if derr := msg.Decode(&got); derr == nil {
				seen[got.User] = true
			}
		case <-deadline:
			t.Fatalf("only %d of 2 notifications fired: %v", len(seen), seen)
		}
	}
}

// Keyspace events fire for every expired key in the database, so a subscriber
// must filter to its own stream and subject.
func TestScheduledSubjectIsHonored(t *testing.T) {
	n := streamsredis.ConnectScheduled(testClient(t), streamsredis.WithPrefix(prefix()))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	_, m := bind(t, n, "wanted", "unwanted")

	msgs, err := m.Subscribe(ctx, "wanted")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, perr := m.Publish(ctx, "unwanted", event{User: "nope"},
		streams.TTL(200*time.Millisecond)); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	select {
	case msg := <-msgs:
		t.Errorf("the subscriber on 'wanted' received an 'unwanted' message: %+v", msg)
	case <-time.After(1500 * time.Millisecond):
		// nothing arrived, as intended
	}
}

// Immediate and scheduled streams share a database but keep separate
// namespaces.
func TestImmediateAndScheduledAreIsolated(t *testing.T) {
	ctx := t.Context()
	rdb := testClient(t)

	p := prefix()
	immediate := streamsredis.Connect(rdb, streamsredis.WithPrefix(p))
	scheduled := streamsredis.ConnectScheduled(rdb, streamsredis.WithPrefix(p))

	if _, err := immediate.Create(ctx, streams.Stream{Name: "plain", Subjects: []string{"s"}}); err != nil {
		t.Fatalf("immediate Create: %v", err)
	}
	if _, err := scheduled.Create(ctx, streams.Stream{Name: "sched", Subjects: []string{"s"}}); err != nil {
		t.Fatalf("scheduled Create: %v", err)
	}

	a, err := immediate.List(ctx)
	if err != nil {
		t.Fatalf("immediate List: %v", err)
	}
	if len(a) != 1 {
		t.Errorf("immediate List returned %d, want 1", len(a))
	}

	b, err := scheduled.List(ctx)
	if err != nil {
		t.Fatalf("scheduled List: %v", err)
	}
	if len(b) != 1 {
		t.Errorf("scheduled List returned %d, want 1", len(b))
	}
}

func TestContextCancellationIsHonored(t *testing.T) {
	s := newStreams(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := s.Get(ctx, "anything"); !errors.Is(err, context.Canceled) {
		t.Errorf("Get with a canceled context = %v, want context.Canceled", err)
	}
}
