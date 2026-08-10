package orm

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/the-protobuf-project/runtime-go/database/core"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

// buildWhere turns an AIP-160 expression into a bound SQL predicate.
//
// This driver used to ignore [store.ListOptions.Filter] outright: a filtered
// listing ran with no WHERE and returned every row. That is the failure the
// contract exists to prevent — the wrong records, with nothing to say anything
// had been dropped — so a filter is now either honored or refused by name.
//
// Values are bound and column names are resolved through the descriptor rather
// than interpolated: a filter routinely arrives from a request, and this is the
// one place in a driver where it reaches a query.
func (d *Driver) buildWhere(res *store.Resource, expr string) (string, []any, error) {
	clauses, err := core.ParseFilter(expr)
	if err != nil {
		return "", nil, fmt.Errorf("gorm: %w", err)
	}
	if len(clauses) == 0 {
		return "", nil, nil
	}

	var (
		parts []string
		args  []any
	)
	for _, c := range clauses {
		col, ok := res.LookupColumn(c.Column)
		if !ok {
			return "", nil, fmt.Errorf(
				"gorm: filter names %q, which resource %q has no column for", c.Column, res.Name)
		}
		typed, cerr := coerce(col.Kind, c.Value)
		if cerr != nil {
			return "", nil, fmt.Errorf("gorm: filter on %q: %w", c.Column, cerr)
		}
		parts = append(parts, fmt.Sprintf("%s %s ?", d.quote(col.Name), sqlOperator(c.Op)))
		args = append(args, typed)
	}
	return strings.Join(parts, " AND "), args, nil
}

// sqlOperator maps a comparison to SQL, where "=" is already right.
func sqlOperator(op string) string {
	if op == "!=" {
		return "<>"
	}
	return op
}

// coerce turns a filter's textual value into the type its column stores, so a
// comparison against a number is a number rather than text the engine has to
// cast on every row.
func coerce(kind store.Kind, raw string) (any, error) {
	switch kind {
	case store.KindInt, store.KindEnum:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is not an integer", raw)
		}
		return n, nil
	case store.KindUint:
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is not an unsigned integer", raw)
		}
		return n, nil
	case store.KindFloat:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is not a number", raw)
		}
		return f, nil
	case store.KindBool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("%q is not a boolean", raw)
		}
		return b, nil
	case store.KindTimestamp:
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return nil, fmt.Errorf("%q is not an RFC 3339 timestamp", raw)
		}
		return t.UTC(), nil
	default:
		return raw, nil
	}
}

// orderClause turns an AIP-132 order expression into SQL.
//
// The column is resolved through the descriptor and quoted, never interpolated.
// OrderBy arrives from a request exactly as Filter does, and passing it to the
// engine verbatim would put request text in the ORDER BY position — the same
// hole this driver closed for filters, left open one clause over.
//
// An unrecognized column orders by the key instead of failing: ordering is a
// presentation concern, and the right records in an unexpected order beat an
// error.
func (d *Driver) orderClause(res *store.Resource, expr string) string {
	fallback := d.quote(res.PKColumn) + " ASC"
	if strings.TrimSpace(expr) == "" {
		return fallback
	}
	fields := strings.Fields(expr)
	col, ok := res.LookupColumn(fields[0])
	if !ok {
		return fallback
	}
	direction := "ASC"
	if len(fields) > 1 && strings.EqualFold(fields[1], "desc") {
		direction = "DESC"
	}
	return d.quote(col.Name) + " " + direction
}
