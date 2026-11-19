package filterx_test

import (
	"encoding/json"
	"testing"

	"github.com/the-protobuf-project/runtime-go/database/filterx"
)

// roomSpec has a JSON field with one declared path (seats, in the typed copy)
// and every other path in the flat_object field.
var roomSpec = filterx.Spec{
	Table: `"search"."rooms"`,
	Fields: map[string]filterx.FieldSpec{
		"attributes": {Column: "attributes", Kind: filterx.KindJSON, JSONKeys: map[string]filterx.Kind{"seats": filterx.KindInt}},
	},
	PK: []filterx.SortSpec{{Column: "id", Kind: filterx.KindText, NotNull: true}},
}

func osQuery(t *testing.T, filter string) string {
	t.Helper()
	conds, err := filterx.Parse(filter)
	if err != nil {
		t.Fatalf("Parse(%q): %v", filter, err)
	}
	q, err := filterx.OpenSearch(roomSpec).Query(conds)
	if err != nil {
		t.Fatalf("Query(%q): %v", filter, err)
	}
	b, err := json.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// OpenSearch's exists sees only a flat_object's first-level keys, so presence
// of an undeclared path also asks for any leaf value there; a declared path is
// a mapped field, where exists is enough.
func TestOpenSearchPresenceOfJSONPaths(t *testing.T) {
	for _, tc := range []struct{ filter, want string }{
		{"attributes.view.sea:*", `{"bool":{"filter":[{"bool":{"minimum_should_match":1,"should":[{"exists":{"field":"attributes.view.sea"}},{"wildcard":{"attributes.view.sea":{"value":"*"}}}]}}]}}`},
		{"attributes.seats:*", `{"bool":{"filter":[{"exists":{"field":"attributes_typed.seats"}}]}}`},
	} {
		if got := osQuery(t, tc.filter); got != tc.want {
			t.Errorf("%s:\n got %s\nwant %s", tc.filter, got, tc.want)
		}
	}
}
