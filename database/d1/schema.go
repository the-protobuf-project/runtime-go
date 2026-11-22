package d1

import (
	"context"
	"fmt"
	"strings"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// EnsureSchema creates the table res describes if it is not already there.
//
// It creates and does not alter: a table that exists with a different shape is
// left exactly as it is, and the mismatch is reported by the first statement
// that cannot run rather than papered over on startup.
func (d *Driver) EnsureSchema(ctx context.Context, res *store.Resource) error {
	if res == nil {
		return fmt.Errorf("d1: EnsureSchema needs a resource")
	}
	if len(res.Columns) == 0 {
		return fmt.Errorf("d1: resource %q describes no columns", res.Name)
	}

	cols := make([]string, 0, len(res.Columns))
	for _, c := range res.Columns {
		def := quote(c.Name) + " " + columnType(c.Kind)
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

	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", quote(res.Table), strings.Join(cols, ", "))
	if _, err := d.run(ctx, Statement{SQL: stmt}); err != nil {
		return fmt.Errorf("d1: cannot create %s: %w", res.Table, err)
	}
	return nil
}

// DropSchema removes the table res describes, and everything in it.
func (d *Driver) DropSchema(ctx context.Context, res *store.Resource) error {
	if res == nil {
		return fmt.Errorf("d1: DropSchema needs a resource")
	}
	if _, err := d.run(ctx, Statement{SQL: "DROP TABLE IF EXISTS " + quote(res.Table)}); err != nil {
		return fmt.Errorf("d1: cannot drop %s: %w", res.Table, err)
	}
	return nil
}

// HasSchema reports whether the table res describes is already there.
func (d *Driver) HasSchema(ctx context.Context, res *store.Resource) (bool, error) {
	if res == nil {
		return false, fmt.Errorf("d1: HasSchema needs a resource")
	}
	out, err := d.run(ctx, Statement{
		SQL:  "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
		Args: []string{res.Table},
	})
	if err != nil {
		return false, err
	}
	return len(out.Rows) > 0, nil
}

// columnType is the SQLite type a kind is stored as.
//
// The type picks the column's affinity, and the affinity is what turns the text
// every value is bound as back into what it was: INTEGER and REAL convert a
// numeric string, TEXT keeps it. Bytes are base64 and timestamps fixed-width
// text, so both are TEXT — a numeric affinity would turn base64 that happens to
// be all digits into a number.
func columnType(k store.Kind) string {
	switch k {
	case store.KindInt, store.KindEnum, store.KindUint, store.KindBool:
		return "INTEGER"
	case store.KindFloat:
		return "REAL"
	default: // KindString, KindBytes, KindTimestamp, KindUnknown
		return "TEXT"
	}
}
