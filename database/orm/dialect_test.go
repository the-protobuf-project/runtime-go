package orm_test

// The dialect matrix.
//
// SQLite runs in-process and is what the rest of this suite uses, but it is the
// most permissive engine here: it accepts ANSI double-quoted identifiers, has no
// strict typing, and forgives column types no other engine has. Two real bugs
// reached this driver behind that permissiveness — identifier quoting that only
// works outside MySQL, and column types that only exist outside PostgreSQL.
//
// These run the same assertions against every SQL engine the driver claims, and
// skip an engine that is not up rather than failing:
//
//	docker compose -f ../docker/compose.yaml up -d postgres mysql

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/the-protobuf-project/runtime-go/database/orm"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

var dialectSeq atomic.Int64

type dialect struct {
	name string
	addr string // empty for an in-process engine
	open func() gorm.Dialector
}

func dialects() []dialect {
	pgHost := envOr("POSTGRES_TEST_ADDR", "127.0.0.1:5434")
	myHost := envOr("MYSQL_TEST_ADDR", "127.0.0.1:3307")
	return []dialect{
		{name: "sqlite", open: func() gorm.Dialector { return sqlite.Open(":memory:") }},
		{
			name: "postgres", addr: pgHost,
			open: func() gorm.Dialector {
				host, port, _ := net.SplitHostPort(pgHost)
				return postgres.Open(fmt.Sprintf(
					"host=%s port=%s user=postgres password=postgres dbname=runtime sslmode=disable", host, port))
			},
		},
		{
			name: "mysql", addr: myHost,
			open: func() gorm.Dialector {
				return mysql.Open(fmt.Sprintf("root:mysql@tcp(%s)/runtime?parseTime=true", myHost))
			},
		},
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// openDialect returns a database on one engine, skipping when it is not up.
func openDialect(t *testing.T, d dialect) *store.DB {
	t.Helper()
	if d.addr != "" {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		var dialer net.Dialer
		conn, err := dialer.DialContext(ctx, "tcp", d.addr)
		if err != nil {
			t.Skipf("no %s at %s", d.name, d.addr)
		}
		_ = conn.Close()
	}

	gdb, err := gorm.Open(d.open(), &gorm.Config{TranslateError: true, Logger: logger.Discard})
	if err != nil {
		t.Skipf("cannot open %s: %v", d.name, err)
	}
	db, err := orm.NewProvider(gdb).SetDatabase(t.Context(), "")
	if err != nil {
		t.Fatalf("%s: SetDatabase: %v", d.name, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// uniqueRes gives each run its own table, so engines with a shared database do
// not collide between tests or between runs, and marks the title unique so the
// constraint is actually exercised — on MySQL a unique column also has to be
// indexable, which a bare TEXT column is not.
func uniqueRes(md protoreflect.MessageDescriptor) *store.Resource {
	res := bookRes(md)
	res.Table = fmt.Sprintf("books_%d_%d", os.Getpid(), dialectSeq.Add(1))
	for i := range res.Columns {
		if res.Columns[i].Name == "title" {
			res.Columns[i].Unique = true
		}
	}
	return res
}

// Everything the driver claims, on every engine it claims it for.
func TestEveryDialect(t *testing.T) {
	for _, d := range dialects() {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			db := openDialect(t, d)
			md := bookMD(t)
			res := uniqueRes(md)

			// EnsureSchema exercises the column types. BLOB is not a PostgreSQL
			// type and TIMESTAMPTZ is not a MySQL one, so a single guess fails
			// here on the engine it was not written for.
			if err := db.Schema.EnsureSchema(ctx, res); err != nil {
				t.Fatalf("EnsureSchema: %v", err)
			}
			t.Cleanup(func() { _ = db.Schema.DropSchema(context.Background(), res) })

			// Get exercises identifier quoting. MySQL reads a double-quoted
			// identifier as a string literal, so a wrongly quoted predicate is
			// always false and every read silently returns nothing.
			if _, err := db.Create(ctx, res, newBook(md, "books/dune", "Dune", 1965, 1)); err != nil {
				t.Fatalf("Create: %v", err)
			}
			got, err := db.Get(ctx, res, "books/dune")
			if err != nil {
				t.Fatalf("Get: %v — the predicate matched nothing", err)
			}
			if title := got.ProtoReflect().Get(md.Fields().ByName("title")).String(); title != "Dune" {
				t.Errorf("title = %q", title)
			}

			// A unique column has to be enforced, which on MySQL means the
			// column must be indexable in the first place. The row is refused,
			// so the counts below do not include it.
			if _, derr := db.Create(ctx, res, newBook(md, "books/other", "Dune", 2021, 1)); !errors.Is(derr, store.ErrAlreadyExists) {
				t.Errorf("duplicate title = %v, want ErrAlreadyExists", derr)
			}

			if _, uerr := db.Update(ctx, res, newBook(md, "books/dune", "Dune (revised)", 1965, 1)); uerr != nil {
				t.Fatalf("Update: %v", uerr)
			}
			got, _ = db.Get(ctx, res, "books/dune")
			if title := got.ProtoReflect().Get(md.Fields().ByName("title")).String(); title != "Dune (revised)" {
				t.Errorf("title after update = %q", title)
			}

			// Filter and OrderBy both build quoted identifiers.
			for i := range 5 {
				id := fmt.Sprintf("books/%02d", i)
				if _, cerr := db.Create(ctx, res, newBook(md, id, id, int32(2000+i), 1)); cerr != nil {
					t.Fatal(cerr)
				}
			}
			out, lerr := db.List(ctx, res, store.ListOptions{
				Filter: "published_year >= 2003", OrderBy: "published_year desc", PageSize: 10,
			})
			if lerr != nil {
				t.Fatalf("List: %v", lerr)
			}
			if len(out.Items) != 2 {
				t.Errorf("filtered list returned %d, want 2", len(out.Items))
			}
			if len(out.Items) > 0 {
				first := out.Items[0].ProtoReflect().Get(md.Fields().ByName("published_year")).Int()
				if first != 2004 {
					t.Errorf("first ordered row = %d, want 2004", first)
				}
			}

			n, err := db.Count(ctx, res, store.ListOptions{Filter: "published_year >= 2003"})
			if err != nil {
				t.Fatalf("Count: %v", err)
			}
			if n != 2 {
				t.Errorf("filtered Count = %d, want 2", n)
			}

			if err := db.Delete(ctx, res, "books/dune"); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if _, err := db.Get(ctx, res, "books/dune"); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("Get after delete = %v, want ErrNotFound", err)
			}
		})
	}
}

// A transaction has to commit and roll back on every engine, not just the one
// that never had to open one.
func TestEveryDialectTransactions(t *testing.T) {
	for _, d := range dialects() {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			db := openDialect(t, d)
			md := bookMD(t)
			res := uniqueRes(md)
			if err := db.Schema.EnsureSchema(ctx, res); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Schema.DropSchema(context.Background(), res) })

			if err := db.Tx.Run(ctx, func(tx *store.DB) error {
				_, cerr := tx.Create(ctx, res, newBook(md, "books/a", "A", 2000, 1))
				return cerr
			}); err != nil {
				t.Fatalf("commit: %v", err)
			}

			boom := errors.New("rolled back")
			_ = db.Tx.Run(ctx, func(tx *store.DB) error {
				if _, cerr := tx.Create(ctx, res, newBook(md, "books/b", "B", 2001, 1)); cerr != nil {
					return cerr
				}
				return boom
			})

			n, err := db.Count(ctx, res, store.ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if n != 1 {
				t.Errorf("%d rows after one commit and one rollback, want 1", n)
			}
		})
	}
}

var _ proto.Message
