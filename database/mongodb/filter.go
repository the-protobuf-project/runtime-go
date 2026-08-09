package mongodb

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/the-protobuf-project/runtime-go/database"
)

// The filter grammar, kept apart from the codec that uses it.
//
// It covers conjunctions of comparisons and nothing else. That is a small
// fraction of AIP-160, and the smallness is the design: a backend that accepts
// the full grammar and honors a subset returns the wrong records with nothing to
// indicate it ignored anything, which is the failure mode this contract exists
// to avoid. Everything else is refused by name.

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
			i += 2 // skip the rest of "AND"
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

// mongoOperator maps a comparison to its Mongo query operator.
func mongoOperator(op string) string {
	switch op {
	case "!=":
		return "$ne"
	case ">":
		return "$gt"
	case ">=":
		return "$gte"
	case "<":
		return "$lt"
	case "<=":
		return "$lte"
	default:
		return "$eq"
	}
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
		if n > 1<<63-1 {
			return nil, fmt.Errorf("%q is too large to compare; values past 2^63 are stored as text", raw)
		}
		return int64(n), nil
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
