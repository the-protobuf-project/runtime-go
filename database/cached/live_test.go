package cached_test

// These exercise the real cache module rather than the fake above, because the
// adapter is where the two contracts meet and the fake cannot prove it. They
// need a live Redis and skip without one:
//
//	docker compose -f ../../cache/docker/compose.yaml up -d redis
//	go test ./...

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/the-protobuf-project/runtime-go/cache"
	rediscache "github.com/the-protobuf-project/runtime-go/cache/redis"
	"github.com/the-protobuf-project/runtime-go/database/cached"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

const dialTimeout = 2 * time.Second

var liveSeq atomic.Int64

func liveAddr() string {
	host, port := os.Getenv("REDIS_TEST_HOST"), os.Getenv("REDIS_TEST_PORT")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "6379"
	}
	return net.JoinHostPort(host, port)
}

// liveCache returns a Cache backed by a real Redis, namespaced per test.
func liveCache(t *testing.T) cached.Cache {
	t.Helper()
	addr := liveAddr()

	ctx, cancel := context.WithTimeout(t.Context(), dialTimeout)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Skipf("no Redis at %s: %v", addr, err)
	}
	_ = conn.Close()

	client, err := rediscache.NewClient(t.Context(), rediscache.Config{Address: addr})
	if err != nil {
		t.Fatalf("cache client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	prefix := fmt.Sprintf("ctest-%d-%d", os.Getpid(), liveSeq.Add(1))
	p := rediscache.New(client, cache.Config{Prefix: prefix, DefaultTTL: time.Minute})
	cdb, err := p.SetDatabase(t.Context(), "records")
	if err != nil {
		t.Fatalf("SetDatabase: %v", err)
	}
	t.Cleanup(func() {
		_, _ = p.DropDatabase(context.Background(), "records")
		_ = cdb.Close()
	})
	return cached.FromAside(cdb, cache.TTL(time.Minute))
}

func liveSetup(t *testing.T) (*store.DB, *fakeStore, *store.Resource, protoreflect.MessageDescriptor) {
	t.Helper()
	md := bookMD(t)
	res := bookRes(md)
	backing := newFakeStore(res)
	db := cached.Wrap(store.Build(backing, "fake", "test", nil), liveCache(t))
	return db, backing, res, md
}

// The adapter's whole job, end to end: a record cached through the real module
// comes back byte for byte, including bytes no JSON encoding would survive.
func TestLiveRoundTrip(t *testing.T) {
	ctx := context.Background()
	db, backing, res, md := liveSetup(t)

	cover := []byte{0x00, 0xff, 0xfe, 0x80, 0x01, 0x0a, 0x22, 0x5c}
	if _, err := db.Create(ctx, res, newBook(md, "books/x", "X", cover)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(ctx, res, "books/x"); err != nil { // populates
		t.Fatal(err)
	}

	got, err := db.Get(ctx, res, "books/x") // served from Redis
	if err != nil {
		t.Fatal(err)
	}
	back := got.ProtoReflect().Get(md.Fields().ByName("cover")).Bytes()
	if !bytes.Equal(back, cover) {
		t.Errorf("cover = %v, want %v", back, cover)
	}
	if title(got, md) != "X" {
		t.Errorf("title = %q", title(got, md))
	}
	if n := backing.gets.Load(); n != 1 {
		t.Errorf("two reads reached the store %d times, want 1", n)
	}
}

// store.ErrNotFound has to survive the trip into the cache's vocabulary and back
// out of it, or the gRPC adapter stops mapping a missing record to NotFound.
func TestLiveNotFoundSurvivesTranslation(t *testing.T) {
	ctx := context.Background()
	db, backing, res, _ := liveSetup(t)

	for range 5 {
		_, err := db.Get(ctx, res, "books/ghost")
		if !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("Get = %v, want store.ErrNotFound", err)
		}
	}
	// And the absence was remembered on the server, not just refused each time.
	if n := backing.gets.Load(); n != 1 {
		t.Errorf("five requests for a missing record reached the store %d times, want 1", n)
	}
}

func TestLiveCreateClearsARememberedAbsence(t *testing.T) {
	ctx := context.Background()
	db, _, res, md := liveSetup(t)

	if _, err := db.Get(ctx, res, "books/new"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("first Get = %v, want ErrNotFound", err)
	}
	if _, err := db.Create(ctx, res, newBook(md, "books/new", "New", nil)); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get(ctx, res, "books/new")
	if err != nil {
		t.Fatalf("the created record is invisible through the real cache: %v", err)
	}
	if title(got, md) != "New" {
		t.Errorf("title = %q", title(got, md))
	}
}

func TestLiveWriteIsVisibleImmediately(t *testing.T) {
	ctx := context.Background()
	db, _, res, md := liveSetup(t)

	if _, err := db.Create(ctx, res, newBook(md, "books/a", "First", nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(ctx, res, "books/a"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Update(ctx, res, newBook(md, "books/a", "Second", nil)); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get(ctx, res, "books/a")
	if err != nil {
		t.Fatal(err)
	}
	if title(got, md) != "Second" {
		t.Errorf("a stale record survived an update: %q", title(got, md))
	}

	if err := db.Delete(ctx, res, "books/a"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(ctx, res, "books/a"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a deleted record was still served: %v", err)
	}
}

// The property the whole cache module was built for, reaching the store layer:
// many callers, one cold key, one load.
func TestLiveConcurrentMissesCollapse(t *testing.T) {
	ctx := context.Background()
	db, backing, res, md := liveSetup(t)

	if _, err := db.Create(ctx, res, newBook(md, "books/hot", "Hot", nil)); err != nil {
		t.Fatal(err)
	}
	backing.hold = make(chan struct{})
	backing.arrived = make(chan struct{})

	const callers = 200
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = db.Get(ctx, res, "books/hot")
		}()
	}
	// Hold the first load until the rest have reached the cache; see the
	// in-process test for why the first arrival is not enough.
	<-backing.arrived
	time.Sleep(100 * time.Millisecond)
	close(backing.hold)
	wg.Wait()

	if n := backing.gets.Load(); n > 3 {
		t.Errorf("%d callers on one cold key caused %d loads, want about 1", callers, n)
	}
}
