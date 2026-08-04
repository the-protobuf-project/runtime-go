package streams_test

// These tests need a live server and skip without one. Notification delivery
// additionally needs `--notify-keyspace-events Ex`, which docker/compose.yaml
// sets:
//
//	docker compose -f ../docker/compose.yaml up -d
//	go test ./...

import (
	"context"
	"errors"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/streams"
)

const subject = "user.login"

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

// newTestProvider returns a provider over a freshly flushed database, skipping
// when no server is listening. Each test gets its own prefix.
func newTestProvider(t *testing.T, prefix string) streams.RedisStreams {
	t.Helper()

	rdb := goredis.NewClient(&goredis.Options{Addr: testAddr(), DB: 7})
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("no Redis at %s (%v); skipping", testAddr(), err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	if err := rdb.FlushDB(t.Context()).Err(); err != nil {
		t.Fatalf("FlushDB: %v", err)
	}

	p, err := streams.Redis(streams.RedisConfig{Client: rdb, Prefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

// newBoundStream creates a stream and binds a manager to it.
func newBoundStream(t *testing.T, p streams.Streams, subjects ...string) (streams.Stream, streams.Manager) {
	t.Helper()

	s, err := p.Create(t.Context(), streams.Stream{Name: "test", Subjects: subjects})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := p.Bind(t.Context(), s.ID())
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	return s, m
}

func TestNewRequiresAClient(t *testing.T) {
	if _, err := streams.Redis(streams.RedisConfig{}); err == nil {
		t.Error("New with no Client returned no error")
	}
}

func TestStreamLifecycle(t *testing.T) {
	p := newTestProvider(t, "lifecycle")
	ctx := t.Context()

	created, err := p.Create(ctx, streams.Stream{
		Name:        "notifications",
		Description: "user events",
		Subjects:    []string{subject},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.ID() == "" {
		t.Fatal("Create did not assign an ID")
	}

	got, err := p.Get(ctx, created.ID())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Description != "user events" {
		t.Errorf("Description = %q, want %q", got.Description, "user events")
	}

	updated, err := p.Update(ctx, created.ID(), streams.Stream{
		Name:        "notifications",
		Description: "changed",
		Subjects:    []string{subject},
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.ID() != created.ID() {
		t.Errorf("Update changed the ID: %q -> %q", created.ID(), updated.ID())
	}

	// Update must leave exactly one metadata entry, not accumulate them.
	reread, err := p.Get(ctx, created.ID())
	if err != nil {
		t.Fatalf("Get after Update: %v", err)
	}
	if reread.Description != "changed" {
		t.Errorf("Get after Update returned the stale metadata: %q", reread.Description)
	}

	list, err := p.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("List returned %d streams, want 1", len(list))
	}

	if err := p.Delete(ctx, created.ID()); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := p.Get(ctx, created.ID()); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Get after Delete = %v, want ErrNotFound", err)
	}
}

// Delete used to log the lookup error and return nil, so a failure looked like
// success — and Update, which deleted before recreating, destroyed the stream.
func TestDeleteOnMissingStreamReportsNotFound(t *testing.T) {
	p := newTestProvider(t, "delete-missing")

	if err := p.Delete(t.Context(), "no-such-stream"); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Delete error = %v, want it to wrap streams.ErrNotFound", err)
	}
}

// Update must not destroy the stream when the replacement cannot be written.
func TestUpdateOnMissingStreamLeavesNothingBehind(t *testing.T) {
	p := newTestProvider(t, "update-missing")
	ctx := t.Context()

	if _, err := p.Update(ctx, "no-such-stream", streams.Stream{
		Name: "x", Subjects: []string{subject},
	}); !errors.Is(err, streams.ErrNotFound) {
		t.Fatalf("Update error = %v, want ErrNotFound", err)
	}

	list, err := p.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("a failed Update created %d streams", len(list))
	}
}

// Bind used to hand back a manager whose client field was never set, so every
// method on it nil-panicked.
func TestBindOnMissingStreamReturnsErrorWithoutPanicking(t *testing.T) {
	p := newTestProvider(t, "bind-missing")

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Bind panicked on a missing stream: %v", r)
		}
	}()

	got, err := p.Bind(t.Context(), "no-such-stream")
	if err == nil {
		t.Fatalf("expected an error, got manager %+v", got)
	}
	if !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("error should wrap ErrNotFound, got %v", err)
	}
}

// Publish must deliver exactly once and must not block. An earlier version
// slept 5s and then published a second copy.
func TestPublishDeliversOnceWithoutSleeping(t *testing.T) {
	p := newTestProvider(t, "publish-once")
	_, m := newBoundStream(t, p, subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	msgs, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	start := time.Now()
	if err := m.Publish(ctx, subject, streams.Message{
		Data: map[string]any{"body": "hello"},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
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

// A subject the stream does not declare must fail loudly at the call that made
// the mistake, rather than creating a channel nobody reads.
func TestUndeclaredSubjectIsRejected(t *testing.T) {
	p := newTestProvider(t, "subjects")
	_, m := newBoundStream(t, p, subject)
	ctx := t.Context()

	err := m.Publish(ctx, "typo.subject", streams.Message{Data: "x"})
	if !errors.Is(err, streams.ErrUnknownSubject) {
		t.Errorf("Publish error = %v, want ErrUnknownSubject", err)
	}

	if _, serr := m.Subscribe(ctx, "typo.subject"); !errors.Is(serr, streams.ErrUnknownSubject) {
		t.Errorf("Subscribe error = %v, want ErrUnknownSubject", serr)
	}
}

// Two subjects on one stream must not cross-deliver.
func TestSubjectsAreIsolated(t *testing.T) {
	p := newTestProvider(t, "isolation")
	_, m := newBoundStream(t, p, "a", "b")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	onA, err := m.Subscribe(ctx, "a")
	if err != nil {
		t.Fatalf("Subscribe(a): %v", err)
	}

	if perr := m.Publish(ctx, "b", streams.Message{Data: "for b"}); perr != nil {
		t.Fatalf("Publish(b): %v", perr)
	}

	select {
	case msg := <-onA:
		t.Errorf("subscriber on 'a' received a message published on 'b': %+v", msg)
	case <-time.After(300 * time.Millisecond):
		// nothing arrived, as intended
	}
}

// Canceling the context must close the channel and release the delivery
// goroutine. Without this the goroutine blocks on its send forever, holding the
// pub/sub connection with it.
func TestCancelingContextClosesTheChannel(t *testing.T) {
	p := newTestProvider(t, "cancel")
	_, m := newBoundStream(t, p, subject)

	ctx, cancel := context.WithCancel(t.Context())
	msgs, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	cancel()

	select {
	case _, ok := <-msgs:
		if ok {
			// A message may already have been queued; the close still has to come.
			select {
			case _, ok := <-msgs:
				if ok {
					t.Error("channel delivered again after cancellation")
				}
			case <-time.After(2 * time.Second):
				t.Error("channel was not closed after cancellation")
			}
		}
	case <-time.After(2 * time.Second):
		t.Error("channel was not closed after cancellation")
	}
}

// A consumer that walks away without draining must not strand the delivery
// goroutine.
func TestAbandonedSubscriberDoesNotLeakGoroutines(t *testing.T) {
	p := newTestProvider(t, "leak")
	_, m := newBoundStream(t, p, subject)

	before := runtime.NumGoroutine()

	func() {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		if _, err := m.Subscribe(ctx, subject); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		// Publish without ever reading: the delivery goroutine blocks on its
		// send until the deferred cancel releases it.
		if err := m.Publish(ctx, subject, streams.Message{Data: "ignored"}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}()

	// Give the goroutine a moment to notice the cancellation and exit.
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

// The message ID the publisher assigns must be the one the subscriber sees.
func TestMessageIDSurvivesDelivery(t *testing.T) {
	p := newTestProvider(t, "ids")
	_, m := newBoundStream(t, p, subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	msgs, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	sent := streams.Message{Data: "hello"}
	sent.SetID("chosen-id")
	if perr := m.Publish(ctx, subject, sent); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	select {
	case got := <-msgs:
		if got.ID() != "chosen-id" {
			t.Errorf("delivered ID = %q, want %q", got.ID(), "chosen-id")
		}
		if got.Data != "hello" {
			t.Errorf("delivered Data = %v, want hello", got.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the message")
	}
}

// A notification with no TTL would never expire, so it could never fire.
// Accepting it silently would strand the subscriber.
func TestNotificationRequiresPositiveTTL(t *testing.T) {
	p := newTestProvider(t, "notify-ttl")
	n := p.Notifications()
	_, m := newBoundStream(t, n, subject)

	err := m.Publish(t.Context(), subject, streams.Message{Data: "no ttl"})
	if err == nil {
		t.Error("Publish with a zero TTL was accepted; it can never fire")
	}
}

// The payload must survive the TTL key it is announced by, and the body must
// be a JSON-serializable value rather than something go-redis refuses.
func TestNotificationDeliversPayloadOnExpiry(t *testing.T) {
	p := newTestProvider(t, "notify-deliver")
	n := p.Notifications()
	_, m := newBoundStream(t, n, subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	msgs, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// A map is exactly what go-redis refuses to marshal directly, which the
	// old implementation passed through raw.
	body := map[string]any{"body": "take a pill"}
	if perr := m.Publish(ctx, subject, streams.Message{
		Data: body,
		TTL:  300 * time.Millisecond,
	}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	select {
	case got, ok := <-msgs:
		if !ok {
			t.Fatal("channel closed before delivering")
		}
		m, isMap := got.Data.(map[string]any)
		if !isMap {
			t.Fatalf("delivered Data is %T, want a map", got.Data)
		}
		if m["body"] != "take a pill" {
			t.Errorf("delivered body = %v, want 'take a pill'", m["body"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("notification never fired (does the server run with --notify-keyspace-events Ex?)")
	}
}

// Each message needs its own pending key. Deriving the key from the message ID
// alone — as an earlier version did — made two notifications overwrite each
// other, so only one ever fired.
func TestTwoNotificationsBothFire(t *testing.T) {
	p := newTestProvider(t, "notify-two")
	n := p.Notifications()
	_, m := newBoundStream(t, n, subject)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	msgs, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	for _, body := range []string{"first", "second"} {
		if perr := m.Publish(ctx, subject, streams.Message{
			Data: body,
			TTL:  300 * time.Millisecond,
		}); perr != nil {
			t.Fatalf("Publish(%s): %v", body, perr)
		}
	}

	seen := map[string]bool{}
	deadline := time.After(5 * time.Second)
	for len(seen) < 2 {
		select {
		case got, ok := <-msgs:
			if !ok {
				t.Fatal("channel closed early")
			}
			if s, isStr := got.Data.(string); isStr {
				seen[s] = true
			}
		case <-deadline:
			t.Fatalf("only %d of 2 notifications fired: %v", len(seen), seen)
		}
	}
}

// Keyspace events fire for every expired key in the database, so a subscriber
// must filter to its own stream and subject rather than taking them all.
func TestNotificationSubjectIsHonored(t *testing.T) {
	p := newTestProvider(t, "notify-subject")
	n := p.Notifications()
	_, m := newBoundStream(t, n, "wanted", "unwanted")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	msgs, err := m.Subscribe(ctx, "wanted")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if perr := m.Publish(ctx, "unwanted", streams.Message{
		Data: "should not arrive",
		TTL:  200 * time.Millisecond,
	}); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	select {
	case got := <-msgs:
		t.Errorf("subscriber on 'wanted' received an 'unwanted' notification: %+v", got)
	case <-time.After(1500 * time.Millisecond):
		// nothing arrived, as intended
	}
}

// Notification streams and ordinary streams live in separate namespaces.
func TestNotificationStreamsAreSeparate(t *testing.T) {
	p := newTestProvider(t, "namespaces")
	ctx := t.Context()

	if _, err := p.Create(ctx, streams.Stream{Name: "plain", Subjects: []string{subject}}); err != nil {
		t.Fatalf("Create(plain): %v", err)
	}
	if _, err := p.Notifications().Create(ctx, streams.Stream{
		Name: "notify", Subjects: []string{subject},
	}); err != nil {
		t.Fatalf("Create(notify): %v", err)
	}

	plain, err := p.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(plain) != 1 {
		t.Errorf("ordinary List returned %d streams, want 1", len(plain))
	}

	notifs, err := p.Notifications().List(ctx)
	if err != nil {
		t.Fatalf("Notifications().List: %v", err)
	}
	if len(notifs) != 1 {
		t.Errorf("notification List returned %d streams, want 1", len(notifs))
	}
}

// A prefix must isolate two providers sharing one database.
func TestPrefixIsolatesProviders(t *testing.T) {
	ctx := t.Context()

	rdb := goredis.NewClient(&goredis.Options{Addr: testAddr(), DB: 8})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("no Redis at %s (%v); skipping", testAddr(), err)
	}
	defer func() { _ = rdb.Close() }()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FlushDB: %v", err)
	}

	a, err := streams.Redis(streams.RedisConfig{Client: rdb, Prefix: "a"})
	if err != nil {
		t.Fatalf("New(a): %v", err)
	}
	b, err := streams.Redis(streams.RedisConfig{Client: rdb, Prefix: "b"})
	if err != nil {
		t.Fatalf("New(b): %v", err)
	}

	created, err := a.Create(ctx, streams.Stream{Name: "x", Subjects: []string{subject}})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, gerr := b.Get(ctx, created.ID()); !errors.Is(gerr, streams.ErrNotFound) {
		t.Errorf("provider b can see provider a's stream (err = %v)", gerr)
	}
	list, err := b.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("provider b lists %d streams, want 0", len(list))
	}
}
