package redis_test

// These tests need a live server and skip without one. Override the target with
// REDIS_TEST_HOST / REDIS_TEST_PORT (default 127.0.0.1:6379):
//
//	docker compose -f ../docker/compose.yaml up -d
//	go test ./...

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/cache"
	cacheredis "github.com/the-protobuf-project/runtime-go/cache/redis"
)

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

// newTestCache returns a cache over a freshly flushed database, skipping when
// no server is listening. Each test gets its own key prefix so they cannot see
// each other's entries.
func newTestCache(t *testing.T, prefix string) *cacheredis.Cache {
	t.Helper()
	ctx := t.Context()

	rdb := goredis.NewClient(&goredis.Options{Addr: testAddr(), DB: 1})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("no Redis at %s (%v); skipping", testAddr(), err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FlushDB: %v", err)
	}

	c, err := cacheredis.New(cacheredis.Config{Client: rdb, Prefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// New must reject a missing client rather than returning a value whose every
// method nil-panics on first use.
func TestNewRequiresAClient(t *testing.T) {
	if _, err := cacheredis.New(cacheredis.Config{}); err == nil {
		t.Error("New with no Client returned no error")
	}
}

func TestCreateGetRoundTrip(t *testing.T) {
	c := newTestCache(t, "roundtrip")
	ctx := t.Context()

	created, err := c.Create(ctx, cache.Document{
		Data: map[string]any{"body": "hello"},
		TTL:  time.Minute,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.ID() == "" {
		t.Fatal("Create did not assign an ID")
	}

	got, err := c.Get(ctx, created.ID())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID() != created.ID() {
		t.Errorf("Get returned ID %q, want %q", got.ID(), created.ID())
	}
	if got.TTL <= 0 || got.TTL > time.Minute {
		t.Errorf("Get returned TTL %v, want a positive duration <= 1m", got.TTL)
	}
}

// Redis answers TTL with -1 for a key with no expiry. Copying that straight
// into Document.TTL contradicts "zero TTL means the entry does not expire", and
// a caller who round-trips it through Update would silently clear the expiry.
func TestGetReportsNoExpiryAsZeroTTL(t *testing.T) {
	c := newTestCache(t, "ttl")
	ctx := t.Context()

	created, err := c.Create(ctx, cache.Document{Data: "forever"}) // no TTL
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := c.Get(ctx, created.ID())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.TTL != 0 {
		t.Errorf("TTL for a non-expiring entry = %v, want 0", got.TTL)
	}
}

// Storage is a plain JSON encode, so scalars and slices must work — not only
// maps and tagged structs, which the previous strict normaliser required.
func TestCreateAcceptsAnyJSONValue(t *testing.T) {
	c := newTestCache(t, "shapes")
	ctx := t.Context()

	for name, data := range map[string]any{
		"string": "plain",
		"number": 42.0,
		"slice":  []any{"a", "b"},
		"map":    map[string]any{"k": "v"},
	} {
		t.Run(name, func(t *testing.T) {
			created, err := c.Create(ctx, cache.Document{Data: data, TTL: time.Minute})
			if err != nil {
				t.Fatalf("Create(%T): %v", data, err)
			}
			if _, err := c.Get(ctx, created.ID()); err != nil {
				t.Errorf("Get after Create(%T): %v", data, err)
			}
		})
	}
}

func TestGetOnMissingEntryReportsNotFound(t *testing.T) {
	c := newTestCache(t, "missing")

	if _, err := c.Get(t.Context(), "no-such-entry"); !errors.Is(err, cache.ErrNotFound) {
		t.Errorf("Get error = %v, want it to wrap cache.ErrNotFound", err)
	}
}

// Update must not resurrect an entry that is gone, and must report it as a
// miss rather than silently creating one.
func TestUpdateOnMissingEntryReportsNotFound(t *testing.T) {
	c := newTestCache(t, "update-missing")
	ctx := t.Context()

	err := c.Update(ctx, "no-such-entry", cache.Document{Data: "x", TTL: time.Minute})
	if !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Update error = %v, want it to wrap cache.ErrNotFound", err)
	}
	if _, err := c.Get(ctx, "no-such-entry"); !errors.Is(err, cache.ErrNotFound) {
		t.Error("Update created the entry it should have rejected")
	}
}

// Create and Delete must agree on the index member they write and remove;
// disagreeing leaks the ID into List forever.
func TestIndexSurvivesCreateDeleteRoundTrip(t *testing.T) {
	c := newTestCache(t, "index")
	ctx := t.Context()

	const id = "//example.com/user/ada"
	if _, err := c.Create(ctx, cache.Document{
		Data: map[string]any{"name": "ada"},
		TTL:  time.Minute,
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	created, err := c.Create(ctx, func() cache.Document {
		d := cache.Document{Data: map[string]any{"name": "ada"}, TTL: time.Minute}
		d.SetID(id)
		return d
	}())
	if err != nil {
		t.Fatalf("Create with resource name: %v", err)
	}

	if derr := c.Delete(ctx, created.ID()); derr != nil {
		t.Fatalf("Delete: %v", derr)
	}

	docs, err := c.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, d := range docs {
		if d.ID() == id {
			t.Errorf("deleted entry %q is still in the index", id)
		}
	}
}

// Entries expire on their own but leave their index member behind; List must
// sweep those rather than let the index grow without bound.
func TestListSweepsExpiredIndexEntries(t *testing.T) {
	c := newTestCache(t, "sweep")
	ctx := t.Context()

	if _, err := c.Create(ctx, cache.Document{
		Data: map[string]any{"body": "brief"},
		TTL:  50 * time.Millisecond,
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := c.Create(ctx, cache.Document{
		Data: map[string]any{"body": "lasting"},
		TTL:  time.Minute,
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	docs, err := c.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("List returned %d documents, want 1 (the expired one should be swept)", len(docs))
	}

	// The sweep must have removed the stale member, not just skipped it.
	again, err := c.List(ctx)
	if err != nil {
		t.Fatalf("second List: %v", err)
	}
	if len(again) != 1 {
		t.Errorf("second List returned %d documents, want 1", len(again))
	}
}

// Deleting something already gone is the caller's intent already satisfied.
func TestDeleteIsIdempotent(t *testing.T) {
	c := newTestCache(t, "delete-twice")

	if err := c.Delete(t.Context(), "never-existed"); err != nil {
		t.Errorf("Delete of a missing entry = %v, want nil", err)
	}
}

// The old package-level clientInstance keyed only on database number, so a
// second client for a different database silently returned the first. Two
// caches built from two different clients must be independent.
func TestCachesFromDifferentClientsAreIndependent(t *testing.T) {
	ctx := t.Context()

	rdb1 := goredis.NewClient(&goredis.Options{Addr: testAddr(), DB: 2})
	if err := rdb1.Ping(ctx).Err(); err != nil {
		_ = rdb1.Close()
		t.Skipf("no Redis at %s (%v); skipping", testAddr(), err)
	}
	defer func() { _ = rdb1.Close() }()

	rdb2 := goredis.NewClient(&goredis.Options{Addr: testAddr(), DB: 3})
	defer func() { _ = rdb2.Close() }()

	if err := rdb1.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FlushDB(2): %v", err)
	}
	if err := rdb2.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FlushDB(3): %v", err)
	}

	c1, err := cacheredis.New(cacheredis.Config{Client: rdb1})
	if err != nil {
		t.Fatalf("New(1): %v", err)
	}
	c2, err := cacheredis.New(cacheredis.Config{Client: rdb2})
	if err != nil {
		t.Fatalf("New(2): %v", err)
	}

	created, err := c1.Create(ctx, cache.Document{Data: "in db 2", TTL: time.Minute})
	if err != nil {
		t.Fatalf("Create on c1: %v", err)
	}

	if _, err := c2.Get(ctx, created.ID()); !errors.Is(err, cache.ErrNotFound) {
		t.Errorf("c2 can see c1's document (err = %v); the caches share state", err)
	}
}

// A prefix must isolate two caches sharing one database.
func TestPrefixIsolatesCaches(t *testing.T) {
	ctx := t.Context()

	rdb := goredis.NewClient(&goredis.Options{Addr: testAddr(), DB: 4})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("no Redis at %s (%v); skipping", testAddr(), err)
	}
	defer func() { _ = rdb.Close() }()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FlushDB: %v", err)
	}

	orders, err := cacheredis.New(cacheredis.Config{Client: rdb, Prefix: "orders"})
	if err != nil {
		t.Fatalf("New(orders): %v", err)
	}
	users, err := cacheredis.New(cacheredis.Config{Client: rdb, Prefix: "users"})
	if err != nil {
		t.Fatalf("New(users): %v", err)
	}

	created, err := orders.Create(ctx, cache.Document{Data: "an order", TTL: time.Minute})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, gerr := users.Get(ctx, created.ID()); !errors.Is(gerr, cache.ErrNotFound) {
		t.Errorf("users cache can see the orders entry (err = %v)", gerr)
	}
	docs, err := users.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(docs) != 0 {
		t.Errorf("users cache lists %d documents, want 0", len(docs))
	}
}

// Every method takes a context; a canceled one must stop the call rather than
// being ignored, which is what the old hardcoded context.Background() did.
func TestContextCancellationIsHonored(t *testing.T) {
	c := newTestCache(t, "ctx")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := c.Get(ctx, "anything"); !errors.Is(err, context.Canceled) {
		t.Errorf("Get with a canceled context = %v, want context.Canceled", err)
	}
}
