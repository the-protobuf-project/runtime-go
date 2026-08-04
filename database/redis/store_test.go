package redis_test

// These tests need a live server and skip without one. Override the target with
// REDIS_TEST_HOST / REDIS_TEST_PORT (default 127.0.0.1:6379):
//
//	docker compose -f ../../cache/docker/compose.yaml up -d
//	go test ./...

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/database"
	dbredis "github.com/the-protobuf-project/runtime-go/database/redis"
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

// newTestClient returns a client over a freshly flushed database, skipping when
// no server is listening.
func newTestClient(t *testing.T) *goredis.Client {
	t.Helper()

	rdb := goredis.NewClient(&goredis.Options{Addr: testAddr(), DB: 5})
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("no Redis at %s (%v); skipping", testAddr(), err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	if err := rdb.FlushDB(t.Context()).Err(); err != nil {
		t.Fatalf("FlushDB: %v", err)
	}
	return rdb
}

// newTestStore returns a store with its own key prefix, so tests cannot see
// each other's documents.
func newTestStore(t *testing.T, prefix string) *dbredis.Store {
	t.Helper()

	s, err := dbredis.New(dbredis.Config{Client: newTestClient(t), Prefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestNewRequiresAClient(t *testing.T) {
	if _, err := dbredis.New(dbredis.Config{}); err == nil {
		t.Error("New with no Client returned no error")
	}
}

func TestCreateGetRoundTrip(t *testing.T) {
	s := newTestStore(t, "roundtrip")
	ctx := t.Context()

	created, err := s.Create(ctx, database.Document{
		Data: map[string]any{"title": "Dune", "year": 1965.0},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.ID() == "" {
		t.Fatal("Create did not assign an ID")
	}

	got, err := s.Get(ctx, created.ID())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	body, ok := got.Data.(map[string]any)
	if !ok {
		t.Fatalf("stored data came back as %T, want a map", got.Data)
	}
	if body["title"] != "Dune" {
		t.Errorf("title = %v, want Dune", body["title"])
	}
}

// Documents are content-addressed: storing the same content twice must resolve
// to one document, not two.
func TestCreateDeduplicatesIdenticalContent(t *testing.T) {
	s := newTestStore(t, "dedup")
	ctx := t.Context()

	content := map[string]any{"title": "Dune", "year": 1965.0}

	first, err := s.Create(ctx, database.Document{Data: content})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	second, err := s.Create(ctx, database.Document{Data: content})
	if err != nil {
		t.Fatalf("second Create: %v", err)
	}

	if first.ID() != second.ID() {
		t.Errorf("identical content stored twice under %q and %q, want one ID",
			first.ID(), second.ID())
	}

	docs, err := s.List(ctx, database.Query{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("store holds %d documents, want 1", len(docs))
	}
}

// Key order is an artifact of how a map was built, not of its content, so it
// must not change the hash.
func TestDeduplicationIgnoresKeyOrder(t *testing.T) {
	s := newTestStore(t, "keyorder")
	ctx := t.Context()

	first, err := s.Create(ctx, database.Document{
		Data: map[string]any{"a": "1", "b": "2", "c": "3"},
	})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	second, err := s.Create(ctx, database.Document{
		Data: map[string]any{"c": "3", "a": "1", "b": "2"},
	})
	if err != nil {
		t.Fatalf("second Create: %v", err)
	}

	if first.ID() != second.ID() {
		t.Error("the same pairs in a different order produced two documents")
	}
}

// The old rollback deleted the content hash unconditionally, including on the
// deduplicated path where the hash belongs to a document this call did not
// create. That silently destroyed the existing document's dedup index and let a
// later Create store a duplicate under a new ID.
func TestDeduplicatedCreateLeavesTheExistingIndexIntact(t *testing.T) {
	s := newTestStore(t, "rollback")
	ctx := t.Context()

	content := map[string]any{"title": "Dune"}

	original, err := s.Create(ctx, database.Document{Data: content})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// A second write of the same content is deduplicated.
	if _, derr := s.Create(ctx, database.Document{Data: content}); derr != nil {
		t.Fatalf("deduplicated Create: %v", derr)
	}

	// If the deduplicated call had rolled back the reservation, this third
	// write would be stored as a brand-new document.
	third, err := s.Create(ctx, database.Document{Data: content})
	if err != nil {
		t.Fatalf("third Create: %v", err)
	}
	if third.ID() != original.ID() {
		t.Errorf("dedup index was destroyed: third write got ID %q, want %q",
			third.ID(), original.ID())
	}
}

func TestGetOnMissingDocumentReportsNotFound(t *testing.T) {
	s := newTestStore(t, "missing")

	if _, err := s.Get(t.Context(), "no-such-doc"); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("Get error = %v, want it to wrap database.ErrNotFound", err)
	}
}

func TestUpdateReplacesContent(t *testing.T) {
	s := newTestStore(t, "update")
	ctx := t.Context()

	created, err := s.Create(ctx, database.Document{Data: map[string]any{"v": "1"}})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if uerr := s.Update(ctx, created.ID(), database.Document{
		Data: map[string]any{"v": "2"},
	}); uerr != nil {
		t.Fatalf("Update: %v", uerr)
	}

	got, err := s.Get(ctx, created.ID())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if body := got.Data.(map[string]any); body["v"] != "2" {
		t.Errorf("v = %v, want 2", body["v"])
	}
}

// After an update the old content must be storable again, and the new content
// must be recognized as a duplicate — i.e. the hash index moved with the write.
func TestUpdateMovesTheContentIndex(t *testing.T) {
	s := newTestStore(t, "reindex")
	ctx := t.Context()

	old := map[string]any{"v": "1"}
	updated := map[string]any{"v": "2"}

	created, err := s.Create(ctx, database.Document{Data: old})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if uerr := s.Update(ctx, created.ID(), database.Document{Data: updated}); uerr != nil {
		t.Fatalf("Update: %v", uerr)
	}

	// The old content is free again.
	reused, err := s.Create(ctx, database.Document{Data: old})
	if err != nil {
		t.Fatalf("Create with the freed content: %v", err)
	}
	if reused.ID() == created.ID() {
		t.Error("the released content still resolves to the updated document")
	}

	// The new content is now claimed by the updated document.
	dup, err := s.Create(ctx, database.Document{Data: updated})
	if err != nil {
		t.Fatalf("Create with the updated content: %v", err)
	}
	if dup.ID() != created.ID() {
		t.Errorf("updated content resolves to %q, want %q", dup.ID(), created.ID())
	}
}

// Two documents must not both claim one content hash.
func TestUpdateToAnotherDocumentsContentIsRejected(t *testing.T) {
	s := newTestStore(t, "conflict")
	ctx := t.Context()

	a, err := s.Create(ctx, database.Document{Data: map[string]any{"v": "a"}})
	if err != nil {
		t.Fatalf("Create(a): %v", err)
	}
	if _, berr := s.Create(ctx, database.Document{Data: map[string]any{"v": "b"}}); berr != nil {
		t.Fatalf("Create(b): %v", berr)
	}

	err = s.Update(ctx, a.ID(), database.Document{Data: map[string]any{"v": "b"}})
	if !errors.Is(err, database.ErrDuplicate) {
		t.Errorf("Update error = %v, want it to wrap database.ErrDuplicate", err)
	}
}

func TestUpdateOnMissingDocumentReportsNotFound(t *testing.T) {
	s := newTestStore(t, "update-missing")

	err := s.Update(t.Context(), "no-such-doc", database.Document{Data: map[string]any{"v": "1"}})
	if !errors.Is(err, database.ErrNotFound) {
		t.Errorf("Update error = %v, want it to wrap database.ErrNotFound", err)
	}
}

// Unlike a cache, a missing record here is a genuine surprise: documents do not
// expire on their own.
func TestDeleteOnMissingDocumentReportsNotFound(t *testing.T) {
	s := newTestStore(t, "delete-missing")

	if err := s.Delete(t.Context(), "no-such-doc"); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("Delete error = %v, want it to wrap database.ErrNotFound", err)
	}
}

// Deleting must release the content hash, or the content could never be stored
// again.
func TestDeleteFreesTheContentForReuse(t *testing.T) {
	s := newTestStore(t, "delete-frees")
	ctx := t.Context()

	content := map[string]any{"title": "Dune"}

	created, err := s.Create(ctx, database.Document{Data: content})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if derr := s.Delete(ctx, created.ID()); derr != nil {
		t.Fatalf("Delete: %v", derr)
	}

	again, err := s.Create(ctx, database.Document{Data: content})
	if err != nil {
		t.Fatalf("Create after Delete: %v", err)
	}
	if again.ID() == created.ID() {
		t.Error("the recreated document reused the deleted ID; the index was not released")
	}

	docs, err := s.List(ctx, database.Query{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("store holds %d documents, want 1", len(docs))
	}
}

// Redis sets have no order, so List sorts before paging — otherwise successive
// pages could overlap or skip documents.
func TestListIsOrderedAndPageable(t *testing.T) {
	s := newTestStore(t, "paging")
	ctx := t.Context()

	for i := range 5 {
		if _, err := s.Create(ctx, database.Document{
			Data: map[string]any{"n": float64(i)},
		}); err != nil {
			t.Fatalf("Create(%d): %v", i, err)
		}
	}

	all, err := s.List(ctx, database.Query{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("List returned %d documents, want 5", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i-1].ID() > all[i].ID() {
			t.Fatalf("List is not sorted by ID: %q before %q", all[i-1].ID(), all[i].ID())
		}
	}

	page, err := s.List(ctx, database.Query{Limit: 2, Offset: 1})
	if err != nil {
		t.Fatalf("List(page): %v", err)
	}
	if len(page) != 2 {
		t.Fatalf("page has %d documents, want 2", len(page))
	}
	if page[0].ID() != all[1].ID() || page[1].ID() != all[2].ID() {
		t.Error("paging did not line up with the full sorted listing")
	}

	if beyond, err := s.List(ctx, database.Query{Offset: 99}); err != nil {
		t.Fatalf("List(beyond): %v", err)
	} else if len(beyond) != 0 {
		t.Errorf("offset past the end returned %d documents, want 0", len(beyond))
	}
}

// A prefix must isolate two stores sharing one database.
func TestPrefixIsolatesStores(t *testing.T) {
	ctx := t.Context()
	rdb := newTestClient(t)

	orders, err := dbredis.New(dbredis.Config{Client: rdb, Prefix: "orders"})
	if err != nil {
		t.Fatalf("New(orders): %v", err)
	}
	users, err := dbredis.New(dbredis.Config{Client: rdb, Prefix: "users"})
	if err != nil {
		t.Fatalf("New(users): %v", err)
	}

	created, err := orders.Create(ctx, database.Document{Data: map[string]any{"o": "1"}})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, gerr := users.Get(ctx, created.ID()); !errors.Is(gerr, database.ErrNotFound) {
		t.Errorf("users store can see the orders document (err = %v)", gerr)
	}

	// Identical content under a different prefix is a different document, since
	// the hash index is namespaced too.
	mirrored, err := users.Create(ctx, database.Document{Data: map[string]any{"o": "1"}})
	if err != nil {
		t.Fatalf("Create in users: %v", err)
	}
	if mirrored.ID() == created.ID() {
		t.Error("dedup crossed the prefix boundary")
	}
}

func TestContextCancellationIsHonored(t *testing.T) {
	s := newTestStore(t, "ctx")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := s.Get(ctx, "anything"); !errors.Is(err, context.Canceled) {
		t.Errorf("Get with a canceled context = %v, want context.Canceled", err)
	}
}
