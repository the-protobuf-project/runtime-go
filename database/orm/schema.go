package orm

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/gorm"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Run executes fn inside a transaction, committing when it returns nil and
// rolling back on any error or panic.
//
// The driver handed to fn is bound to the transaction and carries the same
// schema selection, so a call inside behaves exactly as it would outside except
// for when it becomes visible.
func (d *Driver) Run(ctx context.Context, fn func(*store.DB) error) error {
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// A DB over the transaction-bound driver, so the body reaches the same
		// capabilities it would outside. Nothing was derived to make it, so it
		// carries no Release — the transaction's lifetime is this call.
		bound := &Driver{db: tx, schema: d.schema}
		return fn(store.Build(bound, "gorm", d.schema, nil))
	})
}

// EnsureSchema creates the table res describes if it is not already there.
//
// The descriptor carries the column names, their SQL types, the primary key and
// which columns may not be null, so the statement is derivable rather than
// something a caller writes twice and keeps in sync by hand. That is the whole
// argument for generating the descriptor.
//
// It creates and does not alter. A table that exists with a different shape is
// left exactly as it is: silently rewriting a live table on startup is not a
// thing a library should do, and the mismatch is better reported by the first
// statement that cannot run than papered over here.
func (d *Driver) EnsureSchema(ctx context.Context, res *store.Resource) error {
	if res == nil {
		return fmt.Errorf("gorm: EnsureSchema needs a resource")
	}
	if len(res.Columns) == 0 {
		return fmt.Errorf("gorm: resource %q describes no columns", res.Name)
	}

	cols := make([]string, 0, len(res.Columns))
	for _, c := range res.Columns {
		sqlType := c.SQLType
		if sqlType == "" {
			sqlType = d.defaultSQLType(c.Kind)
		}
		def := d.quote(c.Name) + " " + sqlType
		switch {
		case c.PrimaryKey:
			def += " PRIMARY KEY"
		case c.Unique:
			def += " UNIQUE"
		}
		if c.NotNull && !c.PrimaryKey {
			def += " NOT NULL"
		}
		cols = append(cols, def)
	}

	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", d.quotedTable(res), strings.Join(cols, ", "))
	if err := d.db.WithContext(ctx).Exec(stmt).Error; err != nil {
		return fmt.Errorf("gorm: cannot create %s: %w", d.table(res), err)
	}
	return nil
}

// DropSchema removes the table res describes, and everything in it.
func (d *Driver) DropSchema(ctx context.Context, res *store.Resource) error {
	if res == nil {
		return fmt.Errorf("gorm: DropSchema needs a resource")
	}
	stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s", d.quotedTable(res))
	if err := d.db.WithContext(ctx).Exec(stmt).Error; err != nil {
		return fmt.Errorf("gorm: cannot drop %s: %w", d.table(res), err)
	}
	return nil
}

// HasSchema reports whether the table res describes is already there.
func (d *Driver) HasSchema(ctx context.Context, res *store.Resource) (bool, error) {
	if res == nil {
		return false, fmt.Errorf("gorm: HasSchema needs a resource")
	}
	return d.db.WithContext(ctx).Migrator().HasTable(d.table(res)), nil
}

// defaultSQLType is the column type for a resource whose descriptor names none.
//
// It is dialect-aware because a type name is not portable: BLOB is not a
// PostgreSQL type, TIMESTAMPTZ is not a MySQL one, and BYTEA is neither of the
// others. A single guess creates a table on the engine it was guessed for and
// fails on the rest — which is the failure a caller reading "the same code runs
// against PostgreSQL, MongoDB and TimescaleDB" would least expect.
//
// A descriptor that carries its own SQLType is honored ahead of this: it was
// written for a known target.
func (d *Driver) defaultSQLType(k store.Kind) string {
	postgres := d.dialect("postgres")
	mysql := d.dialect("mysql")

	switch k {
	case store.KindInt, store.KindEnum:
		return "BIGINT"
	case store.KindUint:
		// No engine here has an unsigned 64-bit column that every dialect
		// agrees on, so this is signed and a value past 2^63 will not fit.
		// NUMERIC(20) would hold it on PostgreSQL at the cost of arithmetic
		// nobody asked for; the honest answer is that a counter that large
		// wants its own column type.
		if mysql {
			return "BIGINT UNSIGNED"
		}
		return "BIGINT"
	case store.KindBool:
		return "BOOLEAN"
	case store.KindBytes:
		switch {
		case postgres:
			return "BYTEA"
		case mysql:
			return "LONGBLOB"
		default:
			return "BLOB"
		}
	case store.KindFloat:
		return "DOUBLE PRECISION"
	case store.KindTimestamp:
		switch {
		case postgres:
			return "TIMESTAMPTZ"
		case mysql:
			return "DATETIME(6)"
		default:
			return "TIMESTAMP"
		}
	default: // KindString, KindUnknown
		if mysql {
			// MySQL cannot index a TEXT column without a prefix length, and a
			// unique column has to be indexable.
			return "VARCHAR(255)"
		}
		return "TEXT"
	}
}

// quote wraps an identifier for the dialect in play.
//
// It has to be dialect-aware. MySQL reads a double-quoted string as a literal
// unless ANSI_QUOTES is set, so `"id" = ?` there compares the constant 'id'
// against a value and matches nothing — every read silently returning no rows,
// which is the worst shape a bug can have. Backticks are MySQL's; double quotes
// are everyone else's.
//
// Column names come from a descriptor rather than from a request, so this is
// defense in depth rather than the primary control — but a descriptor is data,
// and data that reaches an identifier position gets quoted.
func (d *Driver) quote(ident string) string {
	if d.dialect("mysql") {
		return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
	}
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// dialect reports whether the connection speaks the named SQL dialect.
func (d *Driver) dialect(name string) bool {
	return strings.Contains(strings.ToLower(d.db.Name()), name)
}
