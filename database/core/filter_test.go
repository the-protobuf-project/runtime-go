package core_test

import (
	"strings"
	"testing"

	"github.com/the-protobuf-project/runtime-go/database/core"
)

// The grammar is shared so that "understood" means the same thing on every
// backend. That only holds if it is exercised, and it had no coverage until
// these.

func TestParseFilter(t *testing.T) {
	for _, tc := range []struct {
		name string
		expr string
		want []core.Clause
	}{
		{"one clause", "age >= 20", []core.Clause{{Column: "age", Op: ">=", Value: "20"}}},
		{"conjunction", "a = 1 AND b < 2", []core.Clause{
			{Column: "a", Op: "=", Value: "1"},
			{Column: "b", Op: "<", Value: "2"},
		}},
		{"lowercase and", "a = 1 and b = 2", []core.Clause{
			{Column: "a", Op: "=", Value: "1"},
			{Column: "b", Op: "=", Value: "2"},
		}},
		{"quoted value", `title = "Dune"`, []core.Clause{{Column: "title", Op: "=", Value: "Dune"}}},
		{"empty expression", "", nil},

		// The longest operator wins at a position, so >= is not read as >.
		{"longest at position", "age >= 20", []core.Clause{{Column: "age", Op: ">=", Value: "20"}}},

		// An operator inside a quoted value is part of the value. Searching for
		// the longest operator anywhere would split here and produce a column
		// nobody declared.
		{"operator inside quotes", `title = "a != b"`, []core.Clause{
			{Column: "title", Op: "=", Value: "a != b"},
		}},
		{"AND inside quotes", `title = "x AND y"`, []core.Clause{
			{Column: "title", Op: "=", Value: "x AND y"},
		}},

		// An equality against the empty string is a real filter, and the three
		// per-driver parsers this replaced all accepted it.
		{"empty string value", `name = ""`, []core.Clause{{Column: "name", Op: "=", Value: ""}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := core.ParseFilter(tc.expr)
			if err != nil {
				t.Fatalf("ParseFilter(%q): %v", tc.expr, err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %d clauses %v, want %d", len(got), got, len(tc.want))
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("clause %d = %+v, want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestParseFilterRefusesWhatItCannotExpress(t *testing.T) {
	for _, expr := range []string{
		"title LIKE Dune",
		"age",
		"= 20",
		"age >=",
		"a = 1 OR b = 2",
	} {
		if _, err := core.ParseFilter(expr); err == nil {
			t.Errorf("ParseFilter(%q) was accepted", expr)
		} else if !strings.Contains(err.Error(), expr) && !strings.Contains(err.Error(), "not understood") {
			t.Errorf("ParseFilter(%q) error does not name the problem: %v", expr, err)
		}
	}
}
