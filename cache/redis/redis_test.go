package redis_test

// These tests need a live server and skip without one. Override the target with
// REDIS_TEST_HOST / REDIS_TEST_PORT (default 127.0.0.1:6379):
//
//	docker compose -f ../../docker/compose.yaml up -d
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

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/cache"
	cacheredis "github.com/the-protobuf-project/runtime-go/cache/redis"
)

// user is a caller's model. Nothing in the provider knows about it — the point
// of having no document type is that this can gain fields freely.
type user struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Age   int    `json:"age"`
}

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

// newCache returns a cache under a prefix unique to this test, so tests share a
// database without seeing each other's entries.
func newCache(t *testing.T) cache.Cache {
	t.Helper()

	rdb := goredis.NewClient(&goredis.Options{Addr: testAddr(), DB: 1})
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("no Redis at %s (%v); skipping", testAddr(), err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	prefix := fmt.Sprintf("t%d_%d", time.Now().UnixNano(), seq.Add(1))
	return cacheredis.Connect(rdb, cacheredis.WithPrefix(prefix))
}

// The caller's model round-trips without a wrapper type.
func TestRoundTripsTheCallersModel(t *testing.T) {
	c := newCache(t)
	ctx := t.Context()

	want := user{Name: "Ada", Email: "ada@example.com", Age: 36}

	id, err := c.Create(ctx, "", want, cache.TTL(time.Minute))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if id == "" {
		t.Fatal("Create did not return an id")
	}

	var got user
	if gerr := c.Get(ctx, id, &got); gerr != nil {
		t.Fatalf("Get: %v", gerr)
	}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

// A caller-chosen id must be used as given, so a resource name can be the key.
func TestCreateHonorsACallerSuppliedID(t *testing.T) {
	c := newCache(t)
	ctx := t.Context()

	const want = "//example.com/user/ada"

	id, err := c.Create(ctx, want, user{Name: "Ada"}, cache.TTL(time.Minute))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if id != want {
		t.Errorf("Create returned id %q, want %q", id, want)
	}
	if gerr := c.Get(ctx, want, &user{}); gerr != nil {
		t.Errorf("the entry is not reachable under the supplied id: %v", gerr)
	}
}

// Redis answers TTL with -1 for a key with no expiry; the contract says zero
// means it does not expire, so that has to be normalized.
func TestReportsNoExpiryAsZeroTTL(t *testing.T) {
	c := newCache(t)
	ctx := t.Context()

	id, err := c.Create(ctx, "", user{Name: "forever"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	ttl, err := c.TTL(ctx, id)
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl != 0 {
		t.Errorf("TTL for a non-expiring entry = %v, want 0", ttl)
	}
}

func TestMissReportsErrNotFound(t *testing.T) {
	c := newCache(t)

	var got user
	if err := c.Get(t.Context(), "absent", &got); !errors.Is(err, cache.ErrNotFound) {
		t.Errorf("Get error = %v, want cache.ErrNotFound", err)
	}
	if _, err := c.TTL(t.Context(), "absent"); !errors.Is(err, cache.ErrNotFound) {
		t.Errorf("TTL error = %v, want cache.ErrNotFound", err)
	}
}

// Update must not resurrect an entry that is gone.
func TestUpdateOnAMissingEntryReportsNotFound(t *testing.T) {
	c := newCache(t)
	ctx := t.Context()

	err := c.Update(ctx, "absent", user{Name: "x"}, cache.TTL(time.Minute))
	if !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Update error = %v, want cache.ErrNotFound", err)
	}

	var got user
	if gerr := c.Get(ctx, "absent", &got); !errors.Is(gerr, cache.ErrNotFound) {
		t.Error("Update created the entry it should have rejected")
	}
}

// Deleting something already gone is the caller's intent already met.
func TestDeleteIsIdempotent(t *testing.T) {
	c := newCache(t)

	if err := c.Delete(t.Context(), "never-existed"); err != nil {
		t.Errorf("Delete of a missing entry = %v, want nil", err)
	}
}

// Entries expire on their own but leave their index member behind; reads must
// sweep those rather than let the index grow without bound.
func TestKeysSweepsExpiredEntries(t *testing.T) {
	c := newCache(t)
	ctx := t.Context()

	if _, err := c.Create(ctx, "", user{Name: "brief"}, cache.TTL(50*time.Millisecond)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := c.Create(ctx, "", user{Name: "lasting"}, cache.TTL(time.Minute)); err != nil {
		t.Fatalf("Create: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	keys, err := c.Keys(ctx)
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(keys) != 1 {
		t.Errorf("Keys returned %d ids, want 1 (the expired one should be swept)", len(keys))
	}
}

// List decodes into whatever concrete slice the caller supplies.
func TestListDecodesIntoTheCallersSlice(t *testing.T) {
	c := newCache(t)
	ctx := t.Context()

	for _, u := range []user{{Name: "a"}, {Name: "b"}} {
		if _, err := c.Create(ctx, "", u, cache.TTL(time.Minute)); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	var got []user
	if err := c.List(ctx, &got); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("List returned %d users, want 2", len(got))
	}
	if got[0].Name == "" {
		t.Error("List returned zero-valued elements; decoding did not happen")
	}
}

// A destination that is not a slice pointer is a programming error and must be
// reported rather than silently ignored.
func TestListRejectsABadDestination(t *testing.T) {
	c := newCache(t)
	ctx := t.Context()

	for _, dest := range []any{nil, user{}, &user{}} {
		if err := c.List(ctx, dest); err == nil {
			t.Errorf("List(%T) returned no error", dest)
		}
	}
}

// The typed view is a wrapper over the same handler — one provider, many models.
func TestTypedViewSharesTheHandler(t *testing.T) {
	c := newCache(t)
	ctx := t.Context()

	users := cache.For[user](c)

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

	// The untyped handler underneath sees the same entry.
	var raw user
	if gerr := c.Get(ctx, id, &raw); gerr != nil {
		t.Errorf("the untyped handler cannot see the typed view's entry: %v", gerr)
	}
}

// A prefix must isolate two caches sharing one client and database.
func TestPrefixIsolatesCaches(t *testing.T) {
	ctx := t.Context()

	rdb := goredis.NewClient(&goredis.Options{Addr: testAddr(), DB: 1})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("no Redis at %s (%v); skipping", testAddr(), err)
	}
	defer func() { _ = rdb.Close() }()

	stamp := time.Now().UnixNano()
	orders := cacheredis.Connect(rdb, cacheredis.WithPrefix(fmt.Sprintf("orders%d", stamp)))
	users := cacheredis.Connect(rdb, cacheredis.WithPrefix(fmt.Sprintf("users%d", stamp)))

	id, err := orders.Create(ctx, "", user{Name: "order"}, cache.TTL(time.Minute))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	var got user
	if gerr := users.Get(ctx, id, &got); !errors.Is(gerr, cache.ErrNotFound) {
		t.Errorf("the users cache can see the orders entry (err = %v)", gerr)
	}
	keys, err := users.Keys(ctx)
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("the users cache lists %d ids, want 0", len(keys))
	}
}

// Two caches over two different clients must be independent — no package-level
// connection state.
func TestCachesFromDifferentClientsAreIndependent(t *testing.T) {
	ctx := t.Context()

	rdb1 := goredis.NewClient(&goredis.Options{Addr: testAddr(), DB: 4})
	if err := rdb1.Ping(ctx).Err(); err != nil {
		_ = rdb1.Close()
		t.Skipf("no Redis at %s (%v); skipping", testAddr(), err)
	}
	defer func() { _ = rdb1.Close() }()

	rdb2 := goredis.NewClient(&goredis.Options{Addr: testAddr(), DB: 5})
	defer func() { _ = rdb2.Close() }()

	prefix := fmt.Sprintf("indep%d", time.Now().UnixNano())
	c1 := cacheredis.Connect(rdb1, cacheredis.WithPrefix(prefix))
	c2 := cacheredis.Connect(rdb2, cacheredis.WithPrefix(prefix))

	id, err := c1.Create(ctx, "", user{Name: "in db 4"}, cache.TTL(time.Minute))
	if err != nil {
		t.Fatalf("Create on c1: %v", err)
	}

	var got user
	if gerr := c2.Get(ctx, id, &got); !errors.Is(gerr, cache.ErrNotFound) {
		t.Errorf("c2 can see c1's entry (err = %v); the caches share state", gerr)
	}
	if gerr := c1.Get(ctx, id, &got); gerr != nil {
		t.Errorf("c1 stopped working after c2 was built: %v", gerr)
	}
}

// Every method takes a context; a canceled one must stop the call.
func TestContextCancellationIsHonored(t *testing.T) {
	c := newCache(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var got user
	if err := c.Get(ctx, "anything", &got); !errors.Is(err, context.Canceled) {
		t.Errorf("Get with a canceled context = %v, want context.Canceled", err)
	}
}
