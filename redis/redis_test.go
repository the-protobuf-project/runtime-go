package redis_test

// These tests need a live server and skip without one. The scheduled-delivery
// tests additionally need `--notify-keyspace-events Ex`, which
// ../cache/docker/compose.yaml sets:
//
//	docker compose -f ../cache/docker/compose.yaml up -d
//	go test ./...

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/redis"
	"github.com/the-protobuf-project/runtime-go/streams"
)

// user is a caller's model. Nothing in the provider knows about it — the point
// of dropping the document wrapper is that this can gain fields freely.
type user struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Age   int    `json:"age"`
}

// dbSeq keeps each test on its own named database so they cannot collide.
var dbSeq atomic.Int64

func testConfig() redis.Config {
	host, port := os.Getenv("REDIS_TEST_HOST"), os.Getenv("REDIS_TEST_PORT")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "6379"
	}
	return redis.Config{Address: host, Port: port}
}

// newClient opens a client, skipping when no server is listening.
func newClient(t *testing.T) *redis.Client {
	t.Helper()

	c, err := redis.New(t.Context(), testConfig())
	if err != nil {
		t.Skipf("no Redis available (%v); skipping", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// newManager walks the whole chain: open, create a database, select it.
func newManager(t *testing.T) *redis.DBManager {
	t.Helper()

	c := newClient(t)
	name := fmt.Sprintf("test_%d_%d", time.Now().UnixNano(), dbSeq.Add(1))

	if err := c.CreateDatabase(t.Context(), name); err != nil {
		t.Fatalf("CreateDatabase(%s): %v", name, err)
	}
	t.Cleanup(func() { _ = c.DeleteDatabase(context.Background(), name) })

	mgr, err := c.SetDatabase(t.Context(), name)
	if err != nil {
		t.Fatalf("SetDatabase(%s): %v", name, err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	return mgr
}

// The chain is the point: a client names a database, the database hands back a
// manager, and the handlers hang off it.
func TestDaisyChain(t *testing.T) {
	mgr := newManager(t)

	if mgr.Document == nil || mgr.Document.Cache == nil || mgr.Document.KV == nil {
		t.Fatal("Document handlers are missing")
	}
	if mgr.Channel == nil || mgr.Channel.Stream == nil || mgr.Channel.Notify == nil {
		t.Fatal("Channel handlers are missing")
	}
	// The registry lives in 0 and 1 is reserved, so a named database starts at 2.
	if mgr.Index() < 2 {
		t.Errorf("database index = %d, want >= 2 (0 is the registry, 1 is reserved)", mgr.Index())
	}
}

func TestCreateDatabaseRejectsADuplicateName(t *testing.T) {
	c := newClient(t)
	name := fmt.Sprintf("dup_%d_%d", time.Now().UnixNano(), dbSeq.Add(1))

	if err := c.CreateDatabase(t.Context(), name); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	defer func() { _ = c.DeleteDatabase(context.Background(), name) }()

	if err := c.CreateDatabase(t.Context(), name); err == nil {
		t.Error("creating the same database twice returned no error")
	}
}

func TestSetDatabaseOnAnUnknownNameFails(t *testing.T) {
	c := newClient(t)

	if _, err := c.SetDatabase(t.Context(), "never_created"); err == nil {
		t.Error("SetDatabase on an unregistered name returned no error")
	}
}

// SetDatabase must return an independent manager, not re-point the client. Two
// managers over different databases have to coexist — the old singleton could
// not do this.
func TestManagersOverDifferentDatabasesAreIndependent(t *testing.T) {
	c := newClient(t)
	ctx := t.Context()

	mgrs := make([]*redis.DBManager, 0, 2)
	for range 2 {
		name := fmt.Sprintf("indep_%d_%d", time.Now().UnixNano(), dbSeq.Add(1))
		if err := c.CreateDatabase(ctx, name); err != nil {
			t.Fatalf("CreateDatabase: %v", err)
		}
		t.Cleanup(func() { _ = c.DeleteDatabase(context.Background(), name) })

		m, err := c.SetDatabase(ctx, name)
		if err != nil {
			t.Fatalf("SetDatabase: %v", err)
		}
		t.Cleanup(func() { _ = m.Close() })
		mgrs = append(mgrs, m)
	}

	if mgrs[0].Index() == mgrs[1].Index() {
		t.Fatalf("both managers bound to database %d", mgrs[0].Index())
	}

	// A write through the first must be invisible to the second, and the first
	// must still work after the second was made.
	id, err := mgrs[0].Document.KV.Create(ctx, "", user{Name: "ada"})
	if err != nil {
		t.Fatalf("Create on the first manager: %v", err)
	}

	var got user
	if err := mgrs[1].Document.KV.Get(ctx, id, &got); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("the second manager can see the first's record (err = %v)", err)
	}
	if aerr := mgrs[0].Document.KV.Get(ctx, id, &got); aerr != nil {
		t.Errorf("the first manager stopped working after a second was made: %v", aerr)
	}
}

// The caller's model round-trips without a wrapper type.
func TestCacheRoundTripsTheCallersModel(t *testing.T) {
	mgr := newManager(t)
	ctx := t.Context()

	want := user{Name: "Ada", Email: "ada@example.com", Age: 36}

	id, err := mgr.Document.Cache.Create(ctx, "", want, cache.TTL(time.Minute))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if id == "" {
		t.Fatal("Create did not return an id")
	}

	var got user
	if gerr := mgr.Document.Cache.Get(ctx, id, &got); gerr != nil {
		t.Fatalf("Get: %v", gerr)
	}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}

	ttl, err := mgr.Document.Cache.TTL(ctx, id)
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 || ttl > time.Minute {
		t.Errorf("TTL = %v, want a positive duration <= 1m", ttl)
	}
}

// Redis answers TTL with -1 for a key with no expiry; the contract says zero
// means it does not expire, so that has to be normalised.
func TestCacheReportsNoExpiryAsZeroTTL(t *testing.T) {
	mgr := newManager(t)
	ctx := t.Context()

	id, err := mgr.Document.Cache.Create(ctx, "", user{Name: "forever"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	ttl, err := mgr.Document.Cache.TTL(ctx, id)
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl != 0 {
		t.Errorf("TTL for a non-expiring entry = %v, want 0", ttl)
	}
}

func TestCacheMissReportsErrNotFound(t *testing.T) {
	mgr := newManager(t)

	var got user
	if err := mgr.Document.Cache.Get(t.Context(), "absent", &got); !errors.Is(err, cache.ErrNotFound) {
		t.Errorf("Get error = %v, want cache.ErrNotFound", err)
	}
}

// Update must not resurrect an entry that is gone.
func TestCacheUpdateOnAMissingEntryReportsNotFound(t *testing.T) {
	mgr := newManager(t)
	ctx := t.Context()

	err := mgr.Document.Cache.Update(ctx, "absent", user{Name: "x"}, cache.TTL(time.Minute))
	if !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Update error = %v, want cache.ErrNotFound", err)
	}

	var got user
	if gerr := mgr.Document.Cache.Get(ctx, "absent", &got); !errors.Is(gerr, cache.ErrNotFound) {
		t.Error("Update created the entry it should have rejected")
	}
}

// Deleting a cache entry that is not there is the caller's intent already met.
func TestCacheDeleteIsIdempotent(t *testing.T) {
	mgr := newManager(t)

	if err := mgr.Document.Cache.Delete(t.Context(), "never-existed"); err != nil {
		t.Errorf("Delete of a missing entry = %v, want nil", err)
	}
}

// Entries expire on their own but leave their index member behind; Keys must
// sweep those rather than let the index grow without bound.
func TestCacheKeysSweepsExpiredEntries(t *testing.T) {
	mgr := newManager(t)
	ctx := t.Context()

	if _, err := mgr.Document.Cache.Create(ctx, "", user{Name: "brief"},
		cache.TTL(50*time.Millisecond)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := mgr.Document.Cache.Create(ctx, "", user{Name: "lasting"},
		cache.TTL(time.Minute)); err != nil {
		t.Fatalf("Create: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	keys, err := mgr.Document.Cache.Keys(ctx)
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(keys) != 1 {
		t.Errorf("Keys returned %d ids, want 1 (the expired one should be swept)", len(keys))
	}
}

// The typed view is a wrapper over the same untyped handler — one client, many
// models.
func TestTypedViewsShareOneHandler(t *testing.T) {
	mgr := newManager(t)
	ctx := t.Context()

	users := cache.For[user](mgr.Document.Cache)

	id, err := users.Create(ctx, user{Name: "Grace", Age: 45}, cache.TTL(time.Minute))
	if err != nil {
		t.Fatalf("typed Create: %v", err)
	}

	got, err := users.Get(ctx, id)
	if err != nil {
		t.Fatalf("typed Get: %v", err)
	}
	if got.Name != "Grace" {
		t.Errorf("Name = %q, want Grace", got.Name)
	}

	all, err := users.List(ctx)
	if err != nil {
		t.Fatalf("typed List: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("typed List returned %d users, want 1", len(all))
	}

	// The untyped handler underneath sees the same entry.
	var raw user
	if err := mgr.Document.Cache.Get(ctx, id, &raw); err != nil {
		t.Errorf("the untyped handler cannot see the typed view's entry: %v", err)
	}
}

// The KV store is content-addressed: identical content resolves to one id.
func TestKVDeduplicatesByContent(t *testing.T) {
	mgr := newManager(t)
	ctx := t.Context()

	u := user{Name: "Dune", Age: 1965}

	first, err := mgr.Document.KV.Create(ctx, "", u)
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	second, err := mgr.Document.KV.Create(ctx, "", u)
	if err != nil {
		t.Fatalf("second Create: %v", err)
	}

	if first != second {
		t.Errorf("identical content stored under %q and %q, want one id", first, second)
	}

	keys, err := mgr.Document.KV.Keys(ctx)
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(keys) != 1 {
		t.Errorf("store holds %d records, want 1", len(keys))
	}
}

// A record's fields are content; the order they were written in is not.
func TestKVDeduplicationIgnoresFieldOrder(t *testing.T) {
	mgr := newManager(t)
	ctx := t.Context()

	first, err := mgr.Document.KV.Create(ctx, "", map[string]any{"a": "1", "b": "2"})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	second, err := mgr.Document.KV.Create(ctx, "", map[string]any{"b": "2", "a": "1"})
	if err != nil {
		t.Fatalf("second Create: %v", err)
	}

	if first != second {
		t.Error("the same pairs in a different order produced two records")
	}
}

// Deleting must release the content, or it could never be stored again.
func TestKVDeleteFreesTheContent(t *testing.T) {
	mgr := newManager(t)
	ctx := t.Context()

	u := user{Name: "released"}

	id, err := mgr.Document.KV.Create(ctx, "", u)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if derr := mgr.Document.KV.Delete(ctx, id); derr != nil {
		t.Fatalf("Delete: %v", derr)
	}

	again, err := mgr.Document.KV.Create(ctx, "", u)
	if err != nil {
		t.Fatalf("Create after Delete: %v", err)
	}
	if again == id {
		t.Error("the recreated record reused the deleted id; the index was not released")
	}
}

// Unlike a cache, a missing record is a genuine surprise.
func TestKVDeleteOnAMissingRecordReportsNotFound(t *testing.T) {
	mgr := newManager(t)

	if err := mgr.Document.KV.Delete(t.Context(), "absent"); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("Delete error = %v, want database.ErrNotFound", err)
	}
}

// Two records must not both claim one content hash.
func TestKVUpdateToAnotherRecordsContentIsRejected(t *testing.T) {
	mgr := newManager(t)
	ctx := t.Context()

	a, err := mgr.Document.KV.Create(ctx, "", user{Name: "a"})
	if err != nil {
		t.Fatalf("Create(a): %v", err)
	}
	if _, berr := mgr.Document.KV.Create(ctx, "", user{Name: "b"}); berr != nil {
		t.Fatalf("Create(b): %v", berr)
	}

	err = mgr.Document.KV.Update(ctx, a, user{Name: "b"})
	if !errors.Is(err, database.ErrDuplicate) {
		t.Errorf("Update error = %v, want database.ErrDuplicate", err)
	}
}

// Sets have no order, so ids are sorted before paging — otherwise successive
// pages would overlap or skip records.
func TestKVKeysArePageable(t *testing.T) {
	mgr := newManager(t)
	ctx := t.Context()

	for i := range 5 {
		if _, err := mgr.Document.KV.Create(ctx, "", user{Name: fmt.Sprintf("u%d", i), Age: i}); err != nil {
			t.Fatalf("Create(%d): %v", i, err)
		}
	}

	all, err := mgr.Document.KV.Keys(ctx)
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("Keys returned %d ids, want 5", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i-1] > all[i] {
			t.Fatalf("Keys is not sorted: %q before %q", all[i-1], all[i])
		}
	}

	page, err := mgr.Document.KV.Keys(ctx, database.Limit(2), database.Offset(1))
	if err != nil {
		t.Fatalf("Keys(page): %v", err)
	}
	if len(page) != 2 || page[0] != all[1] || page[1] != all[2] {
		t.Errorf("page = %v, want %v", page, all[1:3])
	}
}

// Cache and KV share a database but must not see each other's data.
func TestCacheAndKVAreIsolated(t *testing.T) {
	mgr := newManager(t)
	ctx := t.Context()

	cacheID, err := mgr.Document.Cache.Create(ctx, "", user{Name: "cached"}, cache.TTL(time.Minute))
	if err != nil {
		t.Fatalf("cache Create: %v", err)
	}

	var got user
	if err := mgr.Document.KV.Get(ctx, cacheID, &got); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("the KV store can see a cache entry (err = %v)", err)
	}
}

// Publish and subscribe, with the caller's model on both ends.
func TestStreamPublishAndSubscribe(t *testing.T) {
	mgr := newManager(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const subject = "user.created"

	s, err := mgr.Channel.Stream.Create(ctx, streams.Stream{
		Name: "users", Subjects: []string{subject},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	m, err := mgr.Channel.Stream.Bind(ctx, s.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	msgs, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	want := user{Name: "Alan", Age: 41}
	id, err := m.Publish(ctx, subject, want)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case msg := <-msgs:
		if msg.ID != id {
			t.Errorf("delivered id = %q, want %q", msg.ID, id)
		}
		var got user
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

// A subject the stream does not declare must fail at the call that made the
// mistake.
func TestUndeclaredSubjectIsRejected(t *testing.T) {
	mgr := newManager(t)
	ctx := t.Context()

	s, err := mgr.Channel.Stream.Create(ctx, streams.Stream{
		Name: "x", Subjects: []string{"known"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := mgr.Channel.Stream.Bind(ctx, s.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	if _, perr := m.Publish(ctx, "typo", user{}); !errors.Is(perr, streams.ErrUnknownSubject) {
		t.Errorf("Publish error = %v, want ErrUnknownSubject", perr)
	}
	if _, serr := m.Subscribe(ctx, "typo"); !errors.Is(serr, streams.ErrUnknownSubject) {
		t.Errorf("Subscribe error = %v, want ErrUnknownSubject", serr)
	}
}

// An immediate stream cannot honor a delay; saying so beats publishing now and
// letting the caller believe it was scheduled.
func TestImmediateStreamRejectsATTL(t *testing.T) {
	mgr := newManager(t)
	ctx := t.Context()

	s, err := mgr.Channel.Stream.Create(ctx, streams.Stream{
		Name: "x", Subjects: []string{"s"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := mgr.Channel.Stream.Bind(ctx, s.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	if _, perr := m.Publish(ctx, "s", user{}, streams.TTL(time.Second)); perr == nil {
		t.Error("an immediate stream accepted a TTL")
	}
}

// Canceling the context must close the channel and release the delivery
// goroutine.
func TestCancelingClosesTheSubscription(t *testing.T) {
	mgr := newManager(t)
	ctx, cancel := context.WithCancel(t.Context())

	s, err := mgr.Channel.Stream.Create(ctx, streams.Stream{
		Name: "x", Subjects: []string{"s"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := mgr.Channel.Stream.Bind(ctx, s.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	msgs, err := m.Subscribe(ctx, "s")
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

// Notify delivers when the TTL expires rather than on publish.
func TestNotifyDeliversOnExpiry(t *testing.T) {
	mgr := newManager(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const subject = "reminder"

	n, err := mgr.Channel.Notify.Create(ctx, streams.Stream{
		Name: "reminders", Subjects: []string{subject},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := mgr.Channel.Notify.Bind(ctx, n.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	msgs, err := m.Subscribe(ctx, subject)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	want := user{Name: "take a pill"}
	if _, perr := m.Publish(ctx, subject, want, streams.TTL(300*time.Millisecond)); perr != nil {
		t.Fatalf("Publish: %v", perr)
	}

	select {
	case msg := <-msgs:
		var got user
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

// A scheduled message with no TTL could never fire; accepting it silently would
// strand the subscriber.
func TestNotifyRequiresATTL(t *testing.T) {
	mgr := newManager(t)
	ctx := t.Context()

	n, err := mgr.Channel.Notify.Create(ctx, streams.Stream{
		Name: "x", Subjects: []string{"s"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	m, err := mgr.Channel.Notify.Bind(ctx, n.ID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}

	if _, perr := m.Publish(ctx, "s", user{}); perr == nil {
		t.Error("a scheduled message with no TTL was accepted; it can never fire")
	}
}

// Stream and Notify share a database but keep separate namespaces.
func TestStreamAndNotifyAreIsolated(t *testing.T) {
	mgr := newManager(t)
	ctx := t.Context()

	if _, err := mgr.Channel.Stream.Create(ctx, streams.Stream{
		Name: "plain", Subjects: []string{"s"},
	}); err != nil {
		t.Fatalf("Stream Create: %v", err)
	}
	if _, err := mgr.Channel.Notify.Create(ctx, streams.Stream{
		Name: "scheduled", Subjects: []string{"s"},
	}); err != nil {
		t.Fatalf("Notify Create: %v", err)
	}

	plain, err := mgr.Channel.Stream.List(ctx)
	if err != nil {
		t.Fatalf("Stream List: %v", err)
	}
	if len(plain) != 1 {
		t.Errorf("Stream List returned %d, want 1", len(plain))
	}

	scheduled, err := mgr.Channel.Notify.List(ctx)
	if err != nil {
		t.Fatalf("Notify List: %v", err)
	}
	if len(scheduled) != 1 {
		t.Errorf("Notify List returned %d, want 1", len(scheduled))
	}
}

// Update must not destroy the stream, and must leave exactly one metadata entry.
func TestStreamUpdateReplacesMetadata(t *testing.T) {
	mgr := newManager(t)
	ctx := t.Context()

	s, err := mgr.Channel.Stream.Create(ctx, streams.Stream{
		Name: "x", Description: "before", Subjects: []string{"s"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	updated, err := mgr.Channel.Stream.Update(ctx, s.ID, streams.Stream{
		Name: "x", Description: "after", Subjects: []string{"s"},
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.ID != s.ID {
		t.Errorf("Update changed the id: %q -> %q", s.ID, updated.ID)
	}

	got, err := mgr.Channel.Stream.Get(ctx, s.ID)
	if err != nil {
		t.Fatalf("Get after Update: %v", err)
	}
	if got.Description != "after" {
		t.Errorf("Description = %q, want after", got.Description)
	}
}

func TestStreamDeleteOnAMissingStreamReportsNotFound(t *testing.T) {
	mgr := newManager(t)

	if err := mgr.Channel.Stream.Delete(t.Context(), "absent"); !errors.Is(err, streams.ErrNotFound) {
		t.Errorf("Delete error = %v, want streams.ErrNotFound", err)
	}
}

// Every method takes a context; a canceled one must stop the call.
func TestContextCancellationIsHonored(t *testing.T) {
	mgr := newManager(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var got user
	if err := mgr.Document.Cache.Get(ctx, "anything", &got); !errors.Is(err, context.Canceled) {
		t.Errorf("Get with a canceled context = %v, want context.Canceled", err)
	}
}
