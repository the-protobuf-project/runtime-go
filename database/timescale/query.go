package timescale

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/the-protobuf-project/runtime-go/database"
)

// The query fragments, kept apart from the operations that assemble them.
//
// Every value here is bound and every identifier is resolved through the
// descriptor before it is quoted. A filter and a window routinely arrive from a
// request, and a time-series endpoint is exactly the shape that takes one.

// buildWhere assembles the window and the filter into one clause.
func buildWhere(res *database.Resource, filter, timeColumn string, start, end time.Time) (string, []any, error) {
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
func parseFilter(res *database.Resource, expr string) ([]string, []any, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, nil, nil
	}

	var (
		clauses []string
		args    []any
	)
	for _, raw := range splitConjunction(expr) {
		column, op, value, err := parseClause(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("timescale: %w", err)
		}
		col, ok := res.LookupColumn(column)
		if !ok {
			return nil, nil, fmt.Errorf(
				"timescale: filter names %q, which resource %q has no column for", column, res.Name)
		}
		typed, cerr := coerce(col.Kind, value)
		if cerr != nil {
			return nil, nil, fmt.Errorf("timescale: filter on %q: %w", column, cerr)
		}
		clauses = append(clauses, fmt.Sprintf("%s %s ?", quote(col.Name), sqlOperator(op)))
		args = append(args, typed)
	}
	return clauses, args, nil
}

// columnNames resolves a list of column names through the descriptor.
func columnNames(res *database.Resource, names []string) ([]string, error) {
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
func reduction(res *database.Resource, r database.Reduction) (expr, alias string, err error) {
	fn, ok := reducers[r.Func]
	if !ok {
		return "", "", fmt.Errorf(
			"timescale: %q is not a reduction this contract defines; it accepts count, sum, avg, min and max", r.Func)
	}

	if r.Func == database.Count {
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
	case database.KindInt, database.KindUint, database.KindFloat:
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
var reducers = map[database.Reducer]string{
	database.Count: "count",
	database.Sum:   "sum",
	database.Avg:   "avg",
	database.Min:   "min",
	database.Max:   "max",
}

// splitConjunction breaks an expression on AND, outside quotes.
func splitConjunction(expr string) []string {
	var (
		out     []string
		current strings.Builder
		quoteCh rune
	)
	runes := []rune(expr)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch {
		case quoteCh != 0:
			current.WriteRune(r)
			if r == quoteCh {
				quoteCh = 0
			}
		case r == '"' || r == '\'':
			quoteCh = r
			current.WriteRune(r)
		case isAndAt(runes, i):
			out = append(out, current.String())
			current.Reset()
			i += 2
		default:
			current.WriteRune(r)
		}
	}
	if s := strings.TrimSpace(current.String()); s != "" {
		out = append(out, s)
	}
	return out
}

// isAndAt reports whether a bare AND starts at i.
func isAndAt(runes []rune, i int) bool {
	if i+3 > len(runes) {
		return false
	}
	if !strings.EqualFold(string(runes[i:i+3]), "AND") {
		return false
	}
	before := i == 0 || runes[i-1] == ' '
	after := i+3 == len(runes) || runes[i+3] == ' '
	return before && after
}

// operators, longest first so ">=" is not read as ">".
var operators = []string{">=", "<=", "!=", "=", ">", "<"}

// parseClause splits one comparison into its parts.
func parseClause(clause string) (column, op, value string, err error) {
	clause = strings.TrimSpace(clause)
	for _, candidate := range operators {
		if i := strings.Index(clause, candidate); i > 0 {
			column = strings.TrimSpace(clause[:i])
			op = candidate
			value = strings.TrimSpace(clause[i+len(candidate):])
			if column == "" || value == "" {
				return "", "", "", fmt.Errorf("filter clause %q is incomplete", clause)
			}
			return column, op, unquote(value), nil
		}
	}
	return "", "", "", fmt.Errorf(
		"filter clause %q is not understood; this backend accepts conjunctions of `column op value` with = != > >= < <=", clause)
}

// unquote strips a matching pair of surrounding quotes.
func unquote(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
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
func coerce(kind database.Kind, raw string) (any, error) {
	switch kind {
	case database.KindInt, database.KindEnum:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is not an integer", raw)
		}
		return n, nil
	case database.KindUint:
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is not an unsigned integer", raw)
		}
		return n, nil
	case database.KindFloat:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is not a number", raw)
		}
		return f, nil
	case database.KindBool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("%q is not a boolean", raw)
		}
		return b, nil
	case database.KindTimestamp:
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return nil, fmt.Errorf("%q is not an RFC 3339 timestamp", raw)
		}
		return t.UTC(), nil
	default:
		return raw, nil
	}
}
