package core

import (
	"fmt"
	"strings"
)

// The filter grammar, in one place.
//
// Every backend here accepts the same small subset of AIP-160: conjunctions of
// `column op value` with = != > >= < <=. Small because a backend that took the
// whole grammar and honored part of it would return the wrong records with
// nothing to say it had ignored something — which is worse than refusing, and is
// the exact bug a shared parser prevents by making "understood" mean the same
// thing everywhere.
//
// Parsing is shared; translation is not. A clause becomes `$gte` on MongoDB,
// `>=` in SQL and AQL, and the value is coerced by each backend to what its
// column actually stores — so this returns the parse and stops there.

// Clause is one parsed comparison.
type Clause struct {
	// Column is the name as written, still to be resolved against a descriptor:
	// a filter naming a column the resource has no such thing for must be
	// refused rather than reaching a query, and only the driver has the
	// descriptor.
	Column string

	// Op is one of = != > >= < <=.
	Op string

	// Value is the text as written, unquoted but not yet typed.
	Value string
}

// operators, longest first so ">=" is not read as ">".
var operators = []string{">=", "<=", "!=", "=", ">", "<"}

// ParseFilter turns an AIP-160 expression into clauses.
//
// An empty expression is no clauses and no error, which is what lets every
// caller pass opts.Filter straight through without checking it first.
func ParseFilter(expr string) ([]Clause, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, nil
	}
	// A disjunction is refused rather than swallowed. Without this, `a = 1 OR
	// b = 2` parses as the single clause a = "1 OR b = 2" — accepted, coerced,
	// and matching nothing, which is the silent wrong answer this parser exists
	// to prevent.
	if word := bareWord(expr, "OR"); word {
		return nil, fmt.Errorf(
			"filter %q uses OR, which this backend cannot express; it accepts conjunctions of `column op value` joined by AND", expr)
	}

	var out []Clause
	for _, raw := range splitConjunction(expr) {
		c, err := parseClause(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
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

// parseClause splits one comparison into its parts.
//
// The operator is found by scanning left to right outside quotes, taking the
// longest match at the earliest position. Searching for the longest operator
// anywhere instead would split `title = "a != b"` on the != inside the value,
// producing a column nobody declared — so any filter whose text contains =, !,
// < or > would be refused.
func parseClause(clause string) (Clause, error) {
	clause = strings.TrimSpace(clause)
	runes := []rune(clause)

	var quote rune
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if quote != 0 {
			if r == quote {
				quote = 0
			}
			continue
		}
		if r == '"' || r == '\'' {
			quote = r
			continue
		}
		op := operatorAt(runes, i)
		if op == "" || i == 0 {
			continue
		}
		// The value keeps its quotes until after the emptiness check, so that
		// `name = ""` is an equality against the empty string here exactly as it
		// is in every other parser this replaced — not an incomplete clause.
		raw := strings.TrimSpace(string(runes[i+len([]rune(op)):]))
		column := strings.TrimSpace(string(runes[:i]))
		if column == "" || raw == "" {
			return Clause{}, fmt.Errorf("filter clause %q is incomplete", clause)
		}
		return Clause{Column: column, Op: op, Value: unquote(raw)}, nil
	}
	return Clause{}, fmt.Errorf(
		"filter clause %q is not understood; this backend accepts conjunctions of `column op value` with = != > >= < <=", clause)
}

// operatorAt returns the longest operator starting at i, or "".
func operatorAt(runes []rune, i int) string {
	for _, op := range operators {
		or := []rune(op)
		if i+len(or) > len(runes) {
			continue
		}
		if string(runes[i:i+len(or)]) == op {
			return op
		}
	}
	return ""
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

// bareWord reports whether word appears outside quotes as its own token.
func bareWord(expr, word string) bool {
	runes := []rune(expr)
	target := []rune(word)
	var quote rune
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if quote != 0 {
			if r == quote {
				quote = 0
			}
			continue
		}
		if r == '"' || r == '\'' {
			quote = r
			continue
		}
		if i+len(target) > len(runes) {
			continue
		}
		if !strings.EqualFold(string(runes[i:i+len(target)]), word) {
			continue
		}
		before := i == 0 || runes[i-1] == ' '
		after := i+len(target) == len(runes) || runes[i+len(target)] == ' '
		if before && after {
			return true
		}
	}
	return false
}
