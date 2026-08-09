package orm_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"google.golang.org/protobuf/types/dynamicpb"
	"gorm.io/gorm"

	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/database/orm"
)

// openDB returns an empty in-memory database — no table, because the point of
// most of these is that the descriptor creates it.
func openDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{TranslateError: true})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	return db
}

func TestSetDatabaseReportsItsCapabilities(t *testing.T) {
	db, err := orm.NewProvider(openDB(t)).SetDatabase(context.Background(), "")
	if err != nil {
		t.Fatalf("SetDatabase: %v", err)
	}
	defer func() { _ = db.Close() }()

	if db.Backend != "gorm" {
		t.Errorf("Backend = %q, want gorm", db.Backend)
	}
	if db.Tx == nil || db.Schema == nil {
		t.Fatal("capability fields must never be nil")
	}
	if db.Driver == nil {
		t.Fatal("the CRUD driver is missing")
	}
}

// The schema comes from the descriptor, so creating the table is derivable
// rather than something a caller writes twice and keeps in sync by hand.
func TestEnsureSchemaCreatesTheTableFromTheDescriptor(t *testing.T) {
	ctx := context.Background()
	db, err := orm.NewProvider(openDB(t)).SetDatabase(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	md := bookMD(t)
	res := bookRes(md)

	has, herr := db.Schema.HasSchema(ctx, res)
	if herr != nil {
		t.Fatal(herr)
	}
	if has {
		t.Fatal("the table exists before anything created it")
	}

	if serr := db.Schema.EnsureSchema(ctx, res); serr != nil {
		t.Fatalf("EnsureSchema: %v", serr)
	}
	if has, herr = db.Schema.HasSchema(ctx, res); herr != nil || !has {
		t.Fatalf("HasSchema = %v, %v; want true", has, herr)
	}

	// And the table it made actually holds a record.
	if _, cerr := db.Create(ctx, res, newBook(md, "books/dune", "Dune", 1965, 1)); cerr != nil {
		t.Fatalf("Create against the generated table: %v", cerr)
	}
	got, gerr := db.Get(ctx, res, "books/dune")
	if gerr != nil {
		t.Fatalf("Get: %v", gerr)
	}
	if title := got.ProtoReflect().Get(md.Fields().ByName("title")).String(); title != "Dune" {
		t.Errorf("title = %q, want Dune", title)
	}
}

// Called twice it must not fail, because it runs on startup.
func TestEnsureSchemaIsIdempotent(t *testing.T) {
	ctx := context.Background()
	db, _ := orm.NewProvider(openDB(t)).SetDatabase(ctx, "")
	defer func() { _ = db.Close() }()
	res := bookRes(bookMD(t))

	for i := range 3 {
		if err := db.Schema.EnsureSchema(ctx, res); err != nil {
			t.Fatalf("EnsureSchema call %d: %v", i+1, err)
		}
	}
}

func TestDropSchema(t *testing.T) {
	ctx := context.Background()
	db, _ := orm.NewProvider(openDB(t)).SetDatabase(ctx, "")
	defer func() { _ = db.Close() }()
	res := bookRes(bookMD(t))

	if err := db.Schema.EnsureSchema(ctx, res); err != nil {
		t.Fatal(err)
	}
	if err := db.Schema.DropSchema(ctx, res); err != nil {
		t.Fatalf("DropSchema: %v", err)
	}
	if has, err := db.Schema.HasSchema(ctx, res); err != nil || has {
		t.Errorf("HasSchema after drop = %v, %v; want false", has, err)
	}
}

// One set of generated descriptors serving two tenants is the reason
// SetDatabase overrides Resource.Schema. SQLite can ATTACH a second database
// under a name, which is exactly the shape a PostgreSQL schema has.
func TestSelectingADatabaseRedirectsEveryResource(t *testing.T) {
	ctx := context.Background()
	raw := openDB(t)
	if err := raw.Exec(`ATTACH DATABASE ':memory:' AS tenant_a`).Error; err != nil {
		t.Fatalf("attach tenant_a: %v", err)
	}
	if err := raw.Exec(`ATTACH DATABASE ':memory:' AS tenant_b`).Error; err != nil {
		t.Fatalf("attach tenant_b: %v", err)
	}

	p := orm.NewProvider(raw)
	md := bookMD(t)
	res := bookRes(md)

	a, err := p.SetDatabase(ctx, "tenant_a")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	b, err := p.SetDatabase(ctx, "tenant_b")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()

	if err := a.Schema.EnsureSchema(ctx, res); err != nil {
		t.Fatal(err)
	}
	if err := b.Schema.EnsureSchema(ctx, res); err != nil {
		t.Fatal(err)
	}

	if _, err := a.Create(ctx, res, newBook(md, "books/dune", "Dune", 1965, 1)); err != nil {
		t.Fatalf("create in tenant_a: %v", err)
	}

	// The same descriptor, the same key, a different tenant: not there.
	if _, err := b.Get(ctx, res, "books/dune"); !errors.Is(err, database.ErrNotFound) {
		t.Errorf("tenant_b sees tenant_a's record: err = %v, want ErrNotFound", err)
	}
	if _, err := a.Get(ctx, res, "books/dune"); err != nil {
		t.Errorf("tenant_a lost its own record: %v", err)
	}
	if a.Name != "tenant_a" || b.Name != "tenant_b" {
		t.Errorf("names = %q, %q", a.Name, b.Name)
	}
}

func TestTransactionCommits(t *testing.T) {
	ctx := context.Background()
	db, _ := orm.NewProvider(openDB(t)).SetDatabase(ctx, "")
	defer func() { _ = db.Close() }()
	md := bookMD(t)
	res := bookRes(md)
	if err := db.Schema.EnsureSchema(ctx, res); err != nil {
		t.Fatal(err)
	}

	err := db.Tx.Run(ctx, func(tx *database.DB) error {
		if _, cerr := tx.Create(ctx, res, newBook(md, "books/a", "A", 2000, 1)); cerr != nil {
			return cerr
		}
		_, cerr := tx.Create(ctx, res, newBook(md, "books/b", "B", 2001, 1))
		return cerr
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	n, err := db.Count(ctx, res, database.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("committed %d records, want 2", n)
	}
}

// The whole reason Run takes a closure: one exit, and a failure halfway leaves
// nothing behind.
func TestTransactionRollsBack(t *testing.T) {
	ctx := context.Background()
	db, _ := orm.NewProvider(openDB(t)).SetDatabase(ctx, "")
	defer func() { _ = db.Close() }()
	md := bookMD(t)
	res := bookRes(md)
	if err := db.Schema.EnsureSchema(ctx, res); err != nil {
		t.Fatal(err)
	}

	boom := errors.New("second write failed")
	err := db.Tx.Run(ctx, func(tx *database.DB) error {
		if _, cerr := tx.Create(ctx, res, newBook(md, "books/a", "A", 2000, 1)); cerr != nil {
			return cerr
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want the caller's error", err)
	}

	n, err := db.Count(ctx, res, database.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("%d records survived a rolled-back transaction, want 0", n)
	}
}

// A database name reaches an identifier position, and in a multi-tenant program
// it comes from a request.
func TestDatabaseNameIsChecked(t *testing.T) {
	for _, name := range []string{
		`a"; DROP TABLE books; --`,
		"a.b",
		"a b",
		"a-b",
		"1abc",
		strings.Repeat("x", 64),
	} {
		if err := database.CheckDatabaseName(name); err == nil {
			t.Errorf("CheckDatabaseName(%q) was accepted", name)
		}
	}
	for _, name := range []string{"", "orders", "tenant_a", "Tenant1", "_private"} {
		if err := database.CheckDatabaseName(name); err != nil {
			t.Errorf("CheckDatabaseName(%q) = %v, want nil", name, err)
		}
	}

	_, err := orm.NewProvider(openDB(t)).SetDatabase(context.Background(), `a"; DROP TABLE books; --`)
	if err == nil {
		t.Error("SetDatabase accepted an injected identifier")
	}
}

// A capability a backend does not have must refuse by name, not panic.
func TestMissingCapabilitiesRefuseByName(t *testing.T) {
	db := database.Build(bareDriver{}, "chain", "", nil)
	ctx := context.Background()

	err := db.Tx.Run(ctx, func(*database.DB) error { return nil })
	if !errors.Is(err, database.ErrUnimplemented) {
		t.Errorf("Tx.Run = %v, want ErrUnimplemented", err)
	}
	if !strings.Contains(err.Error(), "chain") {
		t.Errorf("the refusal does not name the backend: %v", err)
	}

	if err := db.Schema.EnsureSchema(ctx, nil); !errors.Is(err, database.ErrUnimplemented) {
		t.Errorf("Schema.EnsureSchema = %v, want ErrUnimplemented", err)
	}
}

func TestTypedViewChecksTheDescriptorAgainstTheType(t *testing.T) {
	ctx := context.Background()
	db, _ := orm.NewProvider(openDB(t)).SetDatabase(ctx, "")
	defer func() { _ = db.Close() }()
	md := bookMD(t)
	res := bookRes(md)
	if err := db.Schema.EnsureSchema(ctx, res); err != nil {
		t.Fatal(err)
	}

	books, err := database.For[*dynamicpb.Message](db, res)
	if err != nil {
		t.Fatalf("For: %v", err)
	}
	if _, cerr := books.Create(ctx, newBook(md, "books/dune", "Dune", 1965, 1).(*dynamicpb.Message)); cerr != nil {
		t.Fatalf("Create: %v", cerr)
	}
	got, gerr := books.Get(ctx, "books/dune")
	if gerr != nil {
		t.Fatalf("Get: %v", gerr)
	}
	if title := got.ProtoReflect().Get(md.Fields().ByName("title")).String(); title != "Dune" {
		t.Errorf("title = %q, want Dune", title)
	}

	// A resource with no New cannot allocate, and that is caught at wiring time.
	if _, err := database.For[*dynamicpb.Message](db, &database.Resource{Name: "Broken"}); err == nil {
		t.Error("For accepted a resource with no New")
	}
}

// bareDriver implements the CRUD contract and nothing else, standing in for a
// backend with no transactions and no migrations.
type bareDriver struct{ database.Driver }
