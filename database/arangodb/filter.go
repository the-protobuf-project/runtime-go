package arangodb

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/the-protobuf-project/runtime-go/database/core"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

// buildFilter turns an AIP-160 expression into an AQL FILTER and its bind
// variables.
//
// The same small subset every backend here accepts: conjunctions of
// `column op value` with = != > >= < <=. Small because a backend that took the
// whole grammar and honored part of it would return the wrong records with
// nothing to say it ignored something.
//
// Values are bound, never interpolated, and column names are resolved through
// the descriptor rather than pasted in. A filter routinely arrives from a
// request, so a name this does not recognize is refused rather than reaching the
// query.
func buildFilter(res *store.Resource, expr string) (string, map[string]any, error) {
	binds := map[string]any{}
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", binds, nil
	}

	var parts []string
	clauses, perr := core.ParseFilter(expr)
	if perr != nil {
		return "", nil, fmt.Errorf("arangodb: %w", perr)
	}
	for i, c := range clauses {
		column, op, value := c.Column, c.Op, c.Value
		col, ok := res.LookupColumn(column)
		if !ok {
			return "", nil, fmt.Errorf(
				"arangodb: filter names %q, which resource %q has no column for", column, res.Name)
		}

		field := "doc." + col.Name
		typed, cerr := coerce(col.Kind, value)
		if cerr != nil {
			return "", nil, fmt.Errorf("arangodb: filter on %q: %w", column, cerr)
		}
		if col.PrimaryKey {
			// The key is stored escaped, so a comparison has to be too, or a
			// filter on an id containing a slash silently matches nothing.
			field = "doc._key"
			if s, isString := typed.(string); isString {
				typed = escapeKey(s)
			}
		}

		name := "f" + strconv.Itoa(i)
		binds[name] = typed
		parts = append(parts, fmt.Sprintf("%s %s @%s", field, aqlOperator(op), name))
	}
	return "FILTER " + strings.Join(parts, " AND "), binds, nil
}

// aqlOperator maps a comparison to its AQL form.
func aqlOperator(op string) string {
	if op == "=" {
		return "=="
	}
	return op
}

// coerce turns a filter's textual value into the type its column stores, so a
// comparison against a number is a number rather than a string that never
// matches.
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
		return t.UTC().Format(time.RFC3339Nano), nil
	default:
		return raw, nil
	}
}
