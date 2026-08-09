package orm

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/gorm"

	"github.com/the-protobuf-project/runtime-go/database"
)

// Run executes fn inside a transaction, committing when it returns nil and
// rolling back on any error or panic.
//
// The driver handed to fn is bound to the transaction and carries the same
// schema selection, so a call inside behaves exactly as it would outside except
// for when it becomes visible.
func (d *Driver) Run(ctx context.Context, fn func(*database.DB) error) error {
	return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// A DB over the transaction-bound driver, so the body reaches the same
		// capabilities it would outside. Nothing was derived to make it, so it
		// carries no Release — the transaction's lifetime is this call.
		bound := &Driver{db: tx, schema: d.schema}
		return fn(database.Build(bound, "gorm", d.schema, nil))
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
func (d *Driver) EnsureSchema(ctx context.Context, res *database.Resource) error {
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
			sqlType = defaultSQLType(c.Kind)
		}
		def := quote(c.Name) + " " + sqlType
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

	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", d.table(res), strings.Join(cols, ", "))
	if err := d.db.WithContext(ctx).Exec(stmt).Error; err != nil {
		return fmt.Errorf("gorm: cannot create %s: %w", d.table(res), err)
	}
	return nil
}

// DropSchema removes the table res describes, and everything in it.
func (d *Driver) DropSchema(ctx context.Context, res *database.Resource) error {
	if res == nil {
		return fmt.Errorf("gorm: DropSchema needs a resource")
	}
	stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s", d.table(res))
	if err := d.db.WithContext(ctx).Exec(stmt).Error; err != nil {
		return fmt.Errorf("gorm: cannot drop %s: %w", d.table(res), err)
	}
	return nil
}

// HasSchema reports whether the table res describes is already there.
func (d *Driver) HasSchema(ctx context.Context, res *database.Resource) (bool, error) {
	if res == nil {
		return false, fmt.Errorf("gorm: HasSchema needs a resource")
	}
	return d.db.WithContext(ctx).Migrator().HasTable(d.table(res)), nil
}

// defaultSQLType is the column type for a resource whose descriptor carries only
// a [database.Kind] — a chain-first resource being put in a relational database.
//
// Deliberately conservative. A descriptor generated for SQL carries its own
// SQLType and never reaches here; these are the types that hold the value on
// every engine this driver supports rather than the narrowest that would fit.
func defaultSQLType(k database.Kind) string {
	switch k {
	case database.KindInt, database.KindUint, database.KindEnum:
		return "BIGINT"
	case database.KindBool:
		return "BOOLEAN"
	case database.KindBytes:
		return "BLOB"
	case database.KindFloat:
		return "DOUBLE PRECISION"
	case database.KindTimestamp:
		return "TIMESTAMP"
	default: // KindString, KindUnknown
		return "TEXT"
	}
}

// quote wraps an identifier in double quotes, doubling any it contains.
//
// Column names come from a generated descriptor rather than from a request, so
// this is defense in depth rather than the primary control — but a descriptor
// is data, and data that reaches an identifier position gets quoted.
func quote(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}
