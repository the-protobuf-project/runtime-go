package database_test

import (
	"strings"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

type book struct {
	ID    string    `db:"id,pk"`
	Title string    `db:"title,unique"`
	Year  int32     `db:"published_year"`
	Added time.Time `db:"added_at,autocreate"`
	Cover []byte    `db:"cover"`
	Ratio float64   `db:"ratio"`
	OK    bool      `db:"ok"`
	Count uint64    `db:"count"`

	Ignored string `db:"-"`
	hidden  string //nolint:unused // unexported: never stored
}

func TestDescribeReadsTheStruct(t *testing.T) {
	res, err := database.Describe[book]("books")
	if err != nil {
		t.Fatal(err)
	}
	if res.Name != "book" && res.Name != "Book" {
		t.Errorf("Name = %q", res.Name)
	}
	if res.Table != "books" {
		t.Errorf("Table = %q, want books", res.Table)
	}
	if res.PKColumn != "id" {
		t.Errorf("PKColumn = %q, want id", res.PKColumn)
	}

	byName := map[string]store.Column{}
	for _, c := range res.Columns {
		byName[c.Name] = c
	}
	for name, want := range map[string]store.Kind{
		"id": store.KindString, "title": store.KindString,
		"published_year": store.KindInt, "added_at": store.KindTimestamp,
		"cover": store.KindBytes, "ratio": store.KindFloat,
		"ok": store.KindBool, "count": store.KindUint,
	} {
		got, ok := byName[name]
		if !ok {
			t.Errorf("column %q missing", name)
			continue
		}
		if got.Kind != want {
			t.Errorf("%s is %v, want %v", name, got.Kind, want)
		}
	}
	if !byName["title"].Unique {
		t.Error("title lost its unique tag")
	}
	if !byName["added_at"].AutoCreate {
		t.Error("added_at lost its autocreate tag")
	}
	if _, leaked := byName["-"]; leaked {
		t.Error("a field tagged - was stored")
	}
	if _, leaked := byName["hidden"]; leaked {
		t.Error("an unexported field was stored")
	}
	if _, leaked := byName["ignored"]; leaked {
		t.Error("a field tagged - was stored under its own name")
	}
}

// An untagged struct still works, so a caller trying this out is not stopped by
// tag syntax before anything has run.
func TestUntaggedFieldsTakeSnakeCaseNames(t *testing.T) {
	type plain struct {
		UserID   string `db:",pk"`
		FullName string
	}
	res, err := database.Describe[plain]("")
	if err != nil {
		t.Fatal(err)
	}
	if res.Table != "plains" {
		t.Errorf("Table = %q, want plains", res.Table)
	}
	names := map[string]bool{}
	for _, c := range res.Columns {
		names[c.Name] = true
	}
	if !names["user_id"] || !names["full_name"] {
		t.Errorf("columns = %v, want snake_case", names)
	}
}

// The mistakes worth catching at wiring time rather than at the first write.
func TestDescribeRefusesWhatCannotBeStored(t *testing.T) {
	type noKey struct {
		Name string `db:"name"`
	}
	if _, err := database.Describe[noKey](""); err == nil {
		t.Error("a struct with no pk was accepted")
	} else if !strings.Contains(err.Error(), "pk") {
		t.Errorf("the error does not say what is missing: %v", err)
	}

	type twoKeys struct {
		A string `db:"a,pk"`
		B string `db:"b,pk"`
	}
	if _, err := database.Describe[twoKeys](""); err == nil {
		t.Error("a struct with two pks was accepted")
	}

	type unstorable struct {
		ID  string         `db:"id,pk"`
		Bad map[string]int `db:"bad"`
	}
	if _, err := database.Describe[unstorable](""); err == nil {
		t.Error("a field with no column type was accepted")
	}

	if _, err := database.Describe[int](""); err == nil {
		t.Error("a non-struct was accepted")
	}
}

// Deriving walks the struct and builds a proto descriptor; doing that per call
// would put reflection on the write path.
func TestDescribeIsCached(t *testing.T) {
	a, err := database.Describe[book]("books")
	if err != nil {
		t.Fatal(err)
	}
	b, err := database.Describe[book]("books")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Error("Describe rebuilt the resource instead of caching it")
	}
}
