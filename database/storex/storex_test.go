package storex_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/the-protobuf-project/runtime-go/database/storex"
)

type book struct {
	ID    string `gorm:"primaryKey"`
	Title string
	Year  int
}

func open(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&book{}); err != nil {
		t.Fatal(err)
	}
	return db
}

// The generic store runs CRUD for any model, and is the Store every generated
// store satisfies.
func TestTheGenericStoreRunsCRUD(t *testing.T) {
	ctx := context.Background()
	var s storex.Store[book] = storex.New[book](open(t))

	for i, title := range []string{"Dune", "Emma", "Ulysses"} {
		if err := s.Create(ctx, &book{ID: string(rune('a' + i)), Title: title, Year: 1900 + i}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.GetByID(ctx, "b")
	if err != nil || got.Title != "Emma" {
		t.Fatalf("GetByID = %+v, %v", got, err)
	}
	if _, missErr := s.GetByID(ctx, "z"); !errors.Is(missErr, gorm.ErrRecordNotFound) {
		t.Errorf("a missing id: %v, want gorm.ErrRecordNotFound", missErr)
	}

	page, err := s.List(ctx, storex.ListOptions{Where: "year >= ?", Args: []any{1901}, OrderBy: "year desc", Limit: 1})
	if err != nil || len(page) != 1 || page[0].Title != "Ulysses" {
		t.Errorf("List = %+v, %v", page, err)
	}
	if n, err := s.Count(ctx, storex.ListOptions{Where: "year >= ?", Args: []any{1901}, Limit: 1}); err != nil || n != 2 {
		t.Errorf("Count = %d, %v; want 2, the limit ignored", n, err)
	}

	got.Title = "Emma (revised)"
	if err := s.Update(ctx, got); err != nil {
		t.Fatal(err)
	}
	if again, _ := s.GetByID(ctx, "b"); again.Title != "Emma (revised)" {
		t.Errorf("after Update: %q", again.Title)
	}
	if err := s.DeleteByID(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.Count(ctx, storex.ListOptions{}); n != 2 {
		t.Errorf("after DeleteByID: %d rows, want 2", n)
	}
}

// A lock key separates its parts so two different tuples never collide; a nil
// pointer is an empty part.
func TestLockKeySeparatesItsParts(t *testing.T) {
	year := 1965
	var none *int
	if storex.LockKey("c", "ab", "c") == storex.LockKey("c", "a", "bc") {
		t.Error("two different tuples produced one key")
	}
	if got := storex.LockKey("books_title", "Dune", &year, none); got != "books_title\x1fDune\x1f1965\x1f" {
		t.Errorf("LockKey = %q", got)
	}
}

// A nil telemetry seam is the no-op, which runs the work it is handed.
func TestOrNopRunsTheWork(t *testing.T) {
	ran := false
	err := storex.OrNop(nil).Span(context.Background(), "op", nil, func(context.Context) error {
		ran = true
		return nil
	})
	if err != nil || !ran {
		t.Errorf("Span ran=%v err=%v", ran, err)
	}
	storex.OrNop(nil).RecordOp(context.Background(), "t", "op", time.Millisecond, nil)
}

// EnsureSchemas quotes each name as an identifier, never as SQL.
func TestEnsureSchemasQuotesNames(t *testing.T) {
	db := open(t)
	var stmts []string
	dry := db.Session(&gorm.Session{DryRun: true})
	_ = dry.Callback().Raw().Register("capture", func(tx *gorm.DB) { stmts = append(stmts, tx.Statement.SQL.String()) })
	if err := storex.EnsureSchemas(dry, `pro"mo`); err != nil {
		t.Fatal(err)
	}
	if len(stmts) != 1 || !strings.Contains(stmts[0], `"pro""mo"`) {
		t.Errorf("statements %q, want the name quoted with its quote doubled", stmts)
	}
}
