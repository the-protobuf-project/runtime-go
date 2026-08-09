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
	"github.com/the-protobuf-project/runtime-go/database"
	dbredis "github.com/the-protobuf-project/runtime-go/database/redis"
)

type book struct {
	Title  string `json:"title"`
	Author string `json:"author"`
	Year   int    `json:"year"`
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

// newStore returns a store under a prefix unique to this test.
func newStore(t *testing.T) database.Store {
	t.Helper()

	rdb := goredis.NewClient(&goredis.Options{Addr: testAddr(), DB: 2})
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("no Redis at %s (%v); skipping", testAddr(), err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	prefix := fmt.Sprintf("t%d_%d", time.Now().UnixNano(), seq.Add(1))
	return dbredis.Connect(rdb, dbredis.WithPrefix(prefix))
}

func TestRoundTripsTheCallersModel(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	want := book{Title: "Dune", Author: "Herbert", Year: 1965}

	id, err := s.Create(ctx, "", want)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	var got book
	if gerr := s.Get(ctx, id, &got); gerr != nil {
		t.Fatalf("Get: %v", gerr)
	}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

// The store is content-addressed: identical content resolves to one record.
func TestDeduplicatesByContent(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	b := book{Title: "Dune", Year: 1965}

	first, err := s.Create(ctx, "", b)
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	second, err := s.Create(ctx, "", b)
	if err != nil {
		t.Fatalf("second Create: %v", err)
	}
	if first != second {
		t.Errorf("identical content stored under %q and %q, want one id", first, second)
	}

	keys, err := s.Keys(ctx)
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(keys) != 1 {
		t.Errorf("store holds %d records, want 1", len(keys))
	}
}

// Field order is an artifact of how a value was written, not of its content.
func TestDeduplicationIgnoresFieldOrder(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	first, err := s.Create(ctx, "", map[string]any{"a": "1", "b": "2"})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	second, err := s.Create(ctx, "", map[string]any{"b": "2", "a": "1"})
	if err != nil {
		t.Fatalf("second Create: %v", err)
	}
	if first != second {
		t.Error("the same pairs in a different order produced two records")
	}
}

// A deduplicated Create must not release the existing record's reservation —
// doing so would let a later write store a duplicate under a new id.
func TestDeduplicatedCreateLeavesTheIndexIntact(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	b := book{Title: "Dune"}

	original, err := s.Create(ctx, "", b)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, derr := s.Create(ctx, "", b); derr != nil {
		t.Fatalf("deduplicated Create: %v", derr)
	}

	third, err := s.Create(ctx, "", b)
	if err != nil {
		t.Fatalf("third Create: %v", err)
	}
	if third != original {
		t.Errorf("the dedup index was destroyed: third write got %q, want %q", third, original)
	}
}

func TestGetOnAMissingRecordReportsNotFound(t *testing.T) {
	s := newStore(t)

	var got book
	if err := s.Get(t.Context(), "absent", &got); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("Get error = %v, want database.ErrNotFound", err)
	}
}

// Unlike a cache, a missing record is a genuine surprise.
func TestDeleteOnAMissingRecordReportsNotFound(t *testing.T) {
	s := newStore(t)

	if err := s.Delete(t.Context(), "absent"); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("Delete error = %v, want database.ErrNotFound", err)
	}
}

// Deleting releases the content, or it could never be stored again.
func TestDeleteFreesTheContent(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	b := book{Title: "released"}

	id, err := s.Create(ctx, "", b)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if derr := s.Delete(ctx, id); derr != nil {
		t.Fatalf("Delete: %v", derr)
	}

	again, err := s.Create(ctx, "", b)
	if err != nil {
		t.Fatalf("Create after Delete: %v", err)
	}
	if again == id {
		t.Error("the recreated record reused the deleted id; the index was not released")
	}
}

// After an update the old content is storable again and the new content is
// claimed by the updated record.
func TestUpdateMovesTheContentIndex(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	old := book{Title: "before"}
	updated := book{Title: "after"}

	id, err := s.Create(ctx, "", old)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if uerr := s.Update(ctx, id, updated); uerr != nil {
		t.Fatalf("Update: %v", uerr)
	}

	reused, err := s.Create(ctx, "", old)
	if err != nil {
		t.Fatalf("Create with the freed content: %v", err)
	}
	if reused == id {
		t.Error("the released content still resolves to the updated record")
	}

	dup, err := s.Create(ctx, "", updated)
	if err != nil {
		t.Fatalf("Create with the updated content: %v", err)
	}
	if dup != id {
		t.Errorf("updated content resolves to %q, want %q", dup, id)
	}
}

// Two records must not both claim one content hash.
func TestUpdateToAnotherRecordsContentIsRejected(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	a, err := s.Create(ctx, "", book{Title: "a"})
	if err != nil {
		t.Fatalf("Create(a): %v", err)
	}
	if _, berr := s.Create(ctx, "", book{Title: "b"}); berr != nil {
		t.Fatalf("Create(b): %v", berr)
	}

	if uerr := s.Update(ctx, a, book{Title: "b"}); !errors.Is(uerr, database.ErrDuplicate) {
		t.Errorf("Update error = %v, want database.ErrDuplicate", uerr)
	}
}

func TestUpdateOnAMissingRecordReportsNotFound(t *testing.T) {
	s := newStore(t)

	if err := s.Update(t.Context(), "absent", book{Title: "x"}); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("Update error = %v, want database.ErrNotFound", err)
	}
}

// Sets have no order, so ids are sorted before paging — otherwise successive
// pages would overlap or skip records.
func TestKeysAreSortedAndPageable(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	for i := range 5 {
		if _, err := s.Create(ctx, "", book{Title: fmt.Sprintf("b%d", i), Year: i}); err != nil {
			t.Fatalf("Create(%d): %v", i, err)
		}
	}

	all, err := s.Keys(ctx)
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

	page, err := s.Keys(ctx, database.Limit(2), database.Offset(1))
	if err != nil {
		t.Fatalf("Keys(page): %v", err)
	}
	if len(page) != 2 || page[0] != all[1] || page[1] != all[2] {
		t.Errorf("page = %v, want %v", page, all[1:3])
	}

	if beyond, berr := s.Keys(ctx, database.Offset(99)); berr != nil {
		t.Fatalf("Keys(beyond): %v", berr)
	} else if len(beyond) != 0 {
		t.Errorf("offset past the end returned %d ids, want 0", len(beyond))
	}
}

func TestListDecodesIntoTheCallersSlice(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	for _, b := range []book{{Title: "a"}, {Title: "b"}} {
		if _, err := s.Create(ctx, "", b); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	var got []book
	if err := s.List(ctx, &got, database.Limit(10)); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("List returned %d books, want 2", len(got))
	}
	if got[0].Title == "" {
		t.Error("List returned zero-valued elements; decoding did not happen")
	}
}

// The typed view wraps the same handler.
func TestTypedViewSharesTheHandler(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()

	shelf := database.For[book](s)

	id, err := shelf.Create(ctx, book{Title: "Neuromancer", Year: 1984})
	if err != nil {
		t.Fatalf("typed Create: %v", err)
	}
	got, err := shelf.Get(ctx, id)
	if err != nil {
		t.Fatalf("typed Get: %v", err)
	}
	if got.Title != "Neuromancer" {
		t.Errorf("Title = %q, want Neuromancer", got.Title)
	}

	var raw book
	if gerr := s.Get(ctx, id, &raw); gerr != nil {
		t.Errorf("the untyped handler cannot see the typed view's record: %v", gerr)
	}
}

// A prefix must isolate two stores sharing one client — including their dedup
// indexes, so identical content under two prefixes is two records.
func TestPrefixIsolatesStores(t *testing.T) {
	ctx := t.Context()

	rdb := goredis.NewClient(&goredis.Options{Addr: testAddr(), DB: 2})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("no Redis at %s (%v); skipping", testAddr(), err)
	}
	defer func() { _ = rdb.Close() }()

	stamp := time.Now().UnixNano()
	orders := dbredis.Connect(rdb, dbredis.WithPrefix(fmt.Sprintf("orders%d", stamp)))
	users := dbredis.Connect(rdb, dbredis.WithPrefix(fmt.Sprintf("users%d", stamp)))

	b := book{Title: "shared"}

	a, err := orders.Create(ctx, "", b)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	var got book
	if gerr := users.Get(ctx, a, &got); !errors.Is(gerr, database.ErrNotFound) {
		t.Errorf("the users store can see the orders record (err = %v)", gerr)
	}

	mirrored, err := users.Create(ctx, "", b)
	if err != nil {
		t.Fatalf("Create in users: %v", err)
	}
	if mirrored == a {
		t.Error("dedup crossed the prefix boundary")
	}
}

func TestContextCancellationIsHonored(t *testing.T) {
	s := newStore(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var got book
	if err := s.Get(ctx, "anything", &got); !errors.Is(err, context.Canceled) {
		t.Errorf("Get with a canceled context = %v, want context.Canceled", err)
	}
}
