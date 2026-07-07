package network

import "testing"

func TestBuildBatchTag(t *testing.T) {
	// No arguments: just the aliased field.
	if got := buildBatchTag("m0", "insertThing", nil); got != "m0: insertThing" {
		t.Fatalf("no args: %s", got)
	}
	// Arguments are sorted and their variables are namespaced by the alias so two batched ops
	// with the same argument name (e.g. "objects") never collide on a single $objects variable.
	got := buildBatchTag("m1", "insertThing", map[string]interface{}{
		"objects":   nil,
		"postCheck": nil,
	})
	if want := "m1: insertThing(objects: $m1_objects, postCheck: $m1_postCheck)"; got != want {
		t.Fatalf("with args:\n got %s\nwant %s", got, want)
	}
}
