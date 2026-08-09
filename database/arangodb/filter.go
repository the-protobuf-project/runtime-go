package arangodb

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/the-protobuf-project/runtime-go/database"
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
func buildFilter(res *database.Resource, expr string) (string, map[string]any, error) {
	binds := map[string]any{}
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", binds, nil
	}

	var clauses []string
	for i, raw := range splitConjunction(expr) {
		column, op, value, err := parseClause(raw)
		if err != nil {
			return "", nil, fmt.Errorf("arangodb: %w", err)
		}
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
		clauses = append(clauses, fmt.Sprintf("%s %s @%s", field, aqlOperator(op), name))
	}
	return "FILTER " + strings.Join(clauses, " AND "), binds, nil
}

// splitConjunction breaks an expression on AND, outside quotes.
func splitConjunction(expr string) []string {
	var (
		out     []string
		current strings.Builder
		quote   rune
	)
	runes := []rune(expr)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch {
		case quote != 0:
			current.WriteRune(r)
			if r == quote {
				quote = 0
			}
		case r == '"' || r == '\'':
			quote = r
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
		return t.UTC().Format(time.RFC3339Nano), nil
	default:
		return raw, nil
	}
}
