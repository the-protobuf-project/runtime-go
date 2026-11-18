package repox

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/the-protobuf-project/runtime-go/network/runtime"
)

// service stands in for a project's generated GraphQL client Service.
type service struct{}

func (*service) Tx() *runtime.Tx { return nil }

func TestOnGraphQL(t *testing.T) {
	var unset *service
	for _, tc := range []struct {
		name string
		conn Conn
		want bool
	}{
		{"no handle", Conn{}, false},
		{"a nil *Service", Conn{GraphQL: unset}, false},
		{"a Service", Conn{GraphQL: &service{}}, true},
	} {
		if got := tc.conn.OnGraphQL(); got != tc.want {
			t.Errorf("%s: OnGraphQL() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestSplitName(t *testing.T) {
	ids, err := SplitName("organizations/o1/members/m1", "organizations", "members")
	if err != nil || !slices.Equal(ids, []string{"o1", "m1"}) {
		t.Fatalf("SplitName = %v, %v", ids, err)
	}
	for _, bad := range []string{"organizations/o1", "teams/o1/members/m1", "organizations//members/m1"} {
		if _, err := SplitName(bad, "organizations", "members"); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("SplitName(%q) error = %v, want ErrInvalidArgument", bad, err)
		}
	}
}

func TestNameMatch(t *testing.T) {
	if op, arg := NameMatch("shelves/s1/books/b1"); op != "= ?" || arg != "shelves/s1/books/b1" {
		t.Errorf("exact name: %q %q", op, arg)
	}
	if op, arg := NameMatch("shelves/-/books/b_1"); op != "LIKE ?" || arg != `shelves/%/books/b\_1` {
		t.Errorf("wildcard name: %q %q", op, arg)
	}
}

func TestListByKeysReadsEveryPage(t *testing.T) {
	pages := map[string][]string{"": {"a", "b"}, "p2": {"c"}}
	next := map[string]string{"": "p2"}
	got, err := ListByKeys(context.Background(), "id", []string{"a", "b", "c"}, ListInput{},
		func(_ context.Context, in ListInput) ([]string, string, error) {
			return pages[in.PageToken], next[in.PageToken], nil
		})
	if err != nil || !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("ListByKeys = %v, %v", got, err)
	}
}

type row struct {
	ID   string `gorm:"primaryKey"`
	Name string
}

func memDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&row{}); err != nil {
		t.Fatal(err)
	}
	return db
}

type recorder []string

func (r *recorder) Changed(_ context.Context, name string) { *r = append(*r, name) }

// A Unit on GORM commits its queued writes in one transaction, runs its
// After hooks, and reports every touched name to the Conn's observer.
func TestUnitCommitsOnGorm(t *testing.T) {
	db := memDB(t)
	var seen recorder
	u := NewUnit(Conn{Gorm: db, Changes: &seen})
	u.QueueGorm(nil, func(_ context.Context, tx *gorm.DB) error {
		return tx.Create(&row{ID: "r1", Name: "one"}).Error
	})
	u.Touch("rows/r1")
	after := false
	u.After(func(context.Context) { after = true })
	if err := u.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	var n int64
	db.Model(&row{}).Count(&n)
	if n != 1 || !after || !slices.Equal(seen, []string{"rows/r1"}) {
		t.Fatalf("rows=%d after=%v changed=%v", n, after, seen)
	}
}

// A write that fails rolls back the whole Unit and skips its After hooks.
func TestUnitRollsBackOnFailure(t *testing.T) {
	db := memDB(t)
	u := NewUnit(Conn{Gorm: db})
	u.QueueGorm(nil, func(_ context.Context, tx *gorm.DB) error {
		return tx.Create(&row{ID: "r1"}).Error
	})
	u.QueueGorm(nil, func(_ context.Context, tx *gorm.DB) error {
		return tx.Create(&row{ID: "r1"}).Error // duplicate key
	})
	after := false
	u.After(func(context.Context) { after = true })
	// The error is SQLite's own: MapGormErr maps PostgreSQL's SQLSTATEs.
	if err := u.Commit(context.Background()); err == nil {
		t.Fatal("Commit succeeded, want the duplicate key's error")
	}
	var n int64
	db.Model(&row{}).Count(&n)
	if n != 0 || after {
		t.Fatalf("rows=%d after=%v, want a rollback", n, after)
	}
}
