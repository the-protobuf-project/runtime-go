package d1

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
// Values are bound and column names are resolved through the descriptor rather
// than interpolated: a filter routinely arrives from a request, and this is the
// one place in a driver where it reaches a query. A value is checked against its
// column's kind and then bound in the same text form a write uses, so a filter on
// a timestamp compares fixed-width text against fixed-width text.
func buildWhere(res *store.Resource, expr string) (string, []string, error) {
	clauses, err := core.ParseFilter(expr)
	if err != nil {
		return "", nil, fmt.Errorf("d1: %w", err)
	}
	if len(clauses) == 0 {
		return "", nil, nil
	}

	parts := make([]string, 0, len(clauses))
	args := make([]string, 0, len(clauses))
	for _, c := range clauses {
		col, ok := res.LookupColumn(c.Column)
		if !ok {
			return "", nil, fmt.Errorf(
				"d1: filter names %q, which resource %q has no column for", c.Column, res.Name)
		}
		v, cerr := coerce(col.Kind, c.Value)
		if cerr != nil {
			return "", nil, fmt.Errorf("d1: filter on %q: %w", c.Column, cerr)
		}
		text, _ := bind(v)
		op := c.Op
		if op == "!=" {
			op = "<>"
		}
		parts = append(parts, fmt.Sprintf("%s %s ?", quote(col.Name), op))
		args = append(args, text)
	}
	return strings.Join(parts, " AND "), args, nil
}

// coerce checks a filter's textual value against the kind its column stores.
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
		return t, nil
	default:
		return raw, nil
	}
}

// orderClause turns an AIP-132 order expression into SQL, resolving the column
// through the descriptor so request text never reaches the ORDER BY position.
// An unrecognized column orders by the key: the right records in an unexpected
// order beat an error.
func orderClause(res *store.Resource, expr string) string {
	fallback := quote(res.PKColumn) + " ASC"
	fields := strings.Fields(expr)
	if len(fields) == 0 {
		return fallback
	}
	col, ok := res.LookupColumn(fields[0])
	if !ok {
		return fallback
	}
	direction := "ASC"
	if len(fields) > 1 && strings.EqualFold(fields[1], "desc") {
		direction = "DESC"
	}
	return quote(col.Name) + " " + direction
}
