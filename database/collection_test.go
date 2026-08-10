package database_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/database/orm"
)

// A struct with one field of every kind a backend understands, so a round trip
// proves the conversion both ways rather than only for strings.
type record struct {
	ID    string    `db:"id,pk"`
	Name  string    `db:"name,unique"`
	Count int64     `db:"count"`
	Size  uint64    `db:"size"`
	Ratio float64   `db:"ratio"`
	OK    bool      `db:"ok"`
	Blob  []byte    `db:"blob"`
	When  time.Time `db:"when"`
}

func sqlDB(t *testing.T) *database.DB {
	t.Helper()
	client, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		TranslateError: true, Logger: logger.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := orm.NewProvider(client).SetDatabase(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// The whole point of the package: a Go struct in, the same Go struct out, with
// no protobuf visible anywhere in the calling code.
func TestRoundTrip(t *testing.T) {
	ctx := context.Background()
	c, err := database.Collection[record](sqlDB(t), "records")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}

	want := record{
		ID: "r-1", Name: "first", Count: -42, Size: 1 << 40,
		Ratio: 3.5, OK: true, Blob: []byte{0x00, 0xff, 0xfe},
		When: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC),
	}
	id, err := c.Create(ctx, want)
	if err != nil {
		t.Fatal(err)
	}
	if id != "r-1" {
		t.Errorf("Create returned %q, want r-1", id)
	}

	got, err := c.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != want.Name || got.Count != want.Count || got.Size != want.Size {
		t.Errorf("scalars round-tripped wrong: %+v", got)
	}
	if got.Ratio != want.Ratio || got.OK != want.OK {
		t.Errorf("ratio/bool round-tripped wrong: %+v", got)
	}
	if !bytes.Equal(got.Blob, want.Blob) {
		t.Errorf("blob = %v, want %v", got.Blob, want.Blob)
	}
	if !got.When.Equal(want.When) {
		t.Errorf("time = %v, want %v", got.When, want.When)
	}
}

func TestCRUD(t *testing.T) {
	ctx := context.Background()
	c, err := database.Collection[record](sqlDB(t), "records")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err = c.Create(ctx, record{ID: "a", Name: "one"}); err != nil {
		t.Fatal(err)
	}

	// The unique tag reached the backend and is enforced.
	if _, err = c.Create(ctx, record{ID: "b", Name: "one"}); !errors.Is(err, database.ErrAlreadyExists) {
		t.Errorf("duplicate unique column = %v, want ErrAlreadyExists", err)
	}

	if err = c.Update(ctx, record{ID: "a", Name: "two", Count: 5}); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "two" || got.Count != 5 {
		t.Errorf("after update: %+v", got)
	}

	ok, err := c.Exists(ctx, "a")
	if err != nil || !ok {
		t.Errorf("Exists = %v, %v; want true", ok, err)
	}

	if err = c.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Get(ctx, "a"); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("Get after delete = %v, want ErrNotFound", err)
	}
}

func TestListAndPaging(t *testing.T) {
	ctx := context.Background()
	c, err := database.Collection[record](sqlDB(t), "records")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}

	const n = 25
	for i := range n {
		if _, cerr := c.Create(ctx, record{
			ID: fmt.Sprintf("r-%02d", i), Name: fmt.Sprintf("n-%02d", i), Count: int64(i),
		}); cerr != nil {
			t.Fatal(cerr)
		}
	}

	total, err := c.Count(ctx)
	if err != nil || total != n {
		t.Fatalf("Count = %d, %v; want %d", total, err, n)
	}

	// All pages on the caller's behalf and terminates on the right page.
	all, err := c.All(ctx, database.Page(10))
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != n {
		t.Fatalf("All returned %d, want %d", len(all), n)
	}
	for i := 1; i < len(all); i++ {
		if all[i-1].ID >= all[i].ID {
			t.Fatalf("out of order at %d: %v", i, []string{all[i-1].ID, all[i].ID})
		}
	}

	// And the filter reaches the backend rather than being applied here.
	few, _, err := c.List(ctx, database.Where("count >= 20"), database.Page(50))
	if err != nil {
		t.Fatal(err)
	}
	if len(few) != 5 {
		t.Errorf("filtered list returned %d, want 5", len(few))
	}
}

// The struct layer and the proto layer are the same rows: a record written
// through Coll is readable through the descriptor it derived.
func TestSameRowsAsTheProtoLayer(t *testing.T) {
	ctx := context.Background()
	db := sqlDB(t)
	c, err := database.Collection[record](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Create(ctx, record{ID: "x", Name: "written as a struct"}); err != nil {
		t.Fatal(err)
	}

	// Read back through the layer underneath, with no struct involved.
	msg, err := db.Get(ctx, c.Resource(), "x")
	if err != nil {
		t.Fatalf("the proto layer cannot see what the struct layer wrote: %v", err)
	}
	if msg == nil {
		t.Fatal("no message came back")
	}
}

// generated has its key filled by the driver rather than supplied by the
// caller, which is the only way a caller can learn it.
type generated struct {
	ID      string    `db:"id,pk,ulid"`
	Name    string    `db:"name"`
	AddedAt time.Time `db:"added_at,autocreate"`
}

// Create must return the key the driver generated. The SQL driver used to
// return the caller's own message, so a generated id never came back and the
// very next Get looked up the empty string.
func TestCreateReturnsTheGeneratedKey(t *testing.T) {
	ctx := context.Background()
	c, err := database.Collection[generated](sqlDB(t), "generated")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}

	id, err := c.Create(ctx, generated{Name: "no id supplied"})
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("Create returned an empty key; the generated id never reached the caller")
	}

	got, err := c.Get(ctx, id)
	if err != nil {
		t.Fatalf("the record Create claims it wrote cannot be read back under %q: %v", id, err)
	}
	if got.Name != "no id supplied" {
		t.Errorf("name = %q", got.Name)
	}
	if got.AddedAt.IsZero() {
		t.Error("the autocreate timestamp was not stored")
	}
}
