package timescale

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/the-protobuf-project/runtime-go/database/core"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

// The query fragments, kept apart from the operations that assemble them.
//
// Every value here is bound and every identifier is resolved through the
// descriptor before it is quoted. A filter and a window routinely arrive from a
// request, and a time-series endpoint is exactly the shape that takes one.

// buildWhere assembles the window and the filter into one clause.
func buildWhere(res *store.Resource, filter, timeColumn string, start, end time.Time) (string, []any, error) {
	var (
		clauses []string
		args    []any
	)

	// Inclusive of start and exclusive of end, so consecutive windows tile
	// without overlapping or dropping a row on the boundary.
	if !start.IsZero() {
		clauses = append(clauses, quote(timeColumn)+" >= ?")
		args = append(args, start.UTC())
	}
	if !end.IsZero() {
		clauses = append(clauses, quote(timeColumn)+" < ?")
		args = append(args, end.UTC())
	}

	fc, fa, err := parseFilter(res, filter)
	if err != nil {
		return "", nil, err
	}
	clauses = append(clauses, fc...)
	args = append(args, fa...)

	if len(clauses) == 0 {
		return "", nil, nil
	}
	return "WHERE " + strings.Join(clauses, " AND "), args, nil
}

// parseFilter turns an AIP-160 expression into bound SQL predicates.
//
// The same small subset every backend here accepts: conjunctions of
// `column op value` with = != > >= < <=. Small because a backend that took the
// whole grammar and honored part of it would return the wrong rows with nothing
// to say it ignored something.
func parseFilter(res *store.Resource, expr string) ([]string, []any, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, nil, nil
	}

	var (
		parts []string
		args  []any
	)
	clauses, perr := core.ParseFilter(expr)
	if perr != nil {
		return nil, nil, fmt.Errorf("timescale: %w", perr)
	}
	for _, c := range clauses {
		column, op, value := c.Column, c.Op, c.Value
		col, ok := res.LookupColumn(column)
		if !ok {
			return nil, nil, fmt.Errorf(
				"timescale: filter names %q, which resource %q has no column for", column, res.Name)
		}
		typed, cerr := coerce(col.Kind, value)
		if cerr != nil {
			return nil, nil, fmt.Errorf("timescale: filter on %q: %w", column, cerr)
		}
		parts = append(parts, fmt.Sprintf("%s %s ?", quote(col.Name), sqlOperator(op)))
		args = append(args, typed)
	}
	return parts, args, nil
}

// columnNames resolves a list of column names through the descriptor.
func columnNames(res *store.Resource, names []string) ([]string, error) {
	out := make([]string, 0, len(names))
	for _, name := range names {
		col, ok := res.LookupColumn(name)
		if !ok {
			return nil, fmt.Errorf("timescale: resource %q has no column %q to group by", res.Name, name)
		}
		out = append(out, col.Name)
	}
	return out, nil
}

// reduction renders one reduction and the name its result takes.
func reduction(res *store.Resource, r store.Reduction) (expr, alias string, err error) {
	fn, ok := reducers[r.Func]
	if !ok {
		return "", "", fmt.Errorf(
			"timescale: %q is not a reduction this contract defines; it accepts count, sum, avg, min and max", r.Func)
	}

	if r.Func == store.Count {
		alias = r.As
		if alias == "" {
			alias = "count"
		}
		return "count(*)", alias, nil
	}

	col, found := res.LookupColumn(r.Column)
	if !found {
		return "", "", fmt.Errorf(
			"timescale: resource %q has no column %q to %s", res.Name, r.Column, r.Func)
	}
	switch col.Kind {
	case store.KindInt, store.KindUint, store.KindFloat:
	default:
		return "", "", fmt.Errorf(
			"timescale: %s over %q is not meaningful; it is %v, not a number", r.Func, r.Column, col.Kind)
	}

	alias = r.As
	if alias == "" {
		alias = string(r.Func) + "_" + col.Name
	}
	return fn + "(" + quote(col.Name) + ")", alias, nil
}

// reducers is the closed set, mapped to SQL. Closed because an open one would be
// the backend's own expression language passing through a contract that claims
// to be portable.
var reducers = map[store.Reducer]string{
	store.Count: "count",
	store.Sum:   "sum",
	store.Avg:   "avg",
	store.Min:   "min",
	store.Max:   "max",
}

// sqlOperator maps a comparison to SQL, where "=" is already right.
func sqlOperator(op string) string {
	if op == "!=" {
		return "<>"
	}
	return op
}

// coerce turns a filter's textual value into the type its column stores, so a
// comparison against a number is a number rather than text the database has to
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
