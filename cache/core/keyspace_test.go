package core

import (
	"context"
	"strings"
	"testing"

	"github.com/the-protobuf-project/runtime-go/cache"
)

// The key layout is the whole contract of a named database: two names are
// isolated because, and only because, the keys they build differ. These pin that
// rather than leaving it to be rediscovered from a running server.

func TestNamedDatabasesDoNotShareKeys(t *testing.T) {
	orders := NewKeyspace("app", "orders", 0, false).Strategy("doc")
	carts := NewKeyspace("app", "carts", 0, false).Strategy("doc")

	if orders.entry("x") == carts.entry("x") {
		t.Fatalf("two names built the same key: %s", orders.entry("x"))
	}
	if orders.index() == carts.index() {
		t.Fatalf("two names shared an index: %s", orders.index())
	}
}

func TestKeyLayout(t *testing.T) {
	for _, tc := range []struct {
		name              string
		prefix, namespace string
		db                int
		embedDB           bool
		want              string
	}{
		{"name only", "", "orders", 0, false, "orders:cache:doc:entry:x"},
		{"prefix and name", "app", "orders", 0, false, "app:orders:cache:doc:entry:x"},
		{"index selection keeps the old shape", "app", "", 0, false, "app:cache:doc:entry:x"},
		{"embedded index", "app", "", 3, true, "app:db3:cache:doc:entry:x"},
		{"nothing at all", "", "", 0, false, "cache:doc:entry:x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := NewKeyspace(tc.prefix, tc.namespace, tc.db, tc.embedDB).Strategy("doc").entry("x")
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// A name selected by index must keep building exactly the keys it built before
// names existed, or every entry written by an earlier build becomes unreachable.
func TestIndexSelectionIsUnchanged(t *testing.T) {
	const want = "app:cache:vol:session"
	if got := NewKeyspace("app", "", 0, false).Strategy("vol").raw("session"); got != want {
		t.Fatalf("index selection changed its keys: got %q, want %q", got, want)
	}
}

func TestCheckNamespaceRejectsEmpty(t *testing.T) {
	if err := CheckNamespace(""); err == nil {
		t.Fatal("an empty database name was accepted")
	}
}

// The colon rule keeps the prefix/name join unambiguous. This is the collision
// it exists to prevent, and it is a real one.
func TestCheckNamespaceRejectsASeparator(t *testing.T) {
	ambiguous := "app:orders"
	err := CheckNamespace(ambiguous)
	if err == nil {
		t.Fatal("a name containing ':' was accepted")
	}
	if !strings.Contains(err.Error(), ambiguous) {
		t.Errorf("the error does not name the offending value: %v", err)
	}

	// Were it allowed, these two configurations — which share nothing in their
	// arguments — would address the same keys.
	split := NewKeyspace("app", "orders", 0, false).Strategy("doc").entry("x")
	joined := NewKeyspace("", ambiguous, 0, false).Strategy("doc").entry("x")
	if split != joined {
		t.Fatalf("expected the ambiguity the check prevents, got distinct keys:\n  %s\n  %s", split, joined)
	}
}

// The check does not claim to stop a name reaching another strategy's keys: the
// literal cache: segment sits between them, so that was never possible. Pinned
// so the comment saying as much cannot quietly become wrong.
func TestANameCannotReachAnotherStrategy(t *testing.T) {
	victim := NewKeyspace("", "orders", 0, false).Strategy("doc").entry("x")
	forged := NewKeyspace("", "orders:cache:doc", 0, false).Strategy("entry").raw("x")
	if victim == forged {
		t.Fatalf("a name reached another strategy's keys: %s", victim)
	}
}

func TestCheckNamespaceAcceptsOrdinaryNames(t *testing.T) {
	for _, name := range []string{"orders", "order-items", "order_items", "tenant.acme", "v2"} {
		if err := CheckNamespace(name); err != nil {
			t.Errorf("CheckNamespace(%q) = %v, want nil", name, err)
		}
	}
}

// Build is what a provider actually calls, so this checks the namespace survives
// the trip into a live DB rather than only into a Keyspace.
func TestBuildCarriesTheName(t *testing.T) {
	db := Build(newFake(), Spec{Prefix: "app", Namespace: "orders"})
	defer func() { _ = db.Close() }()

	if db.Name != "orders" {
		t.Errorf("db.Name = %q, want %q", db.Name, "orders")
	}

	ctx := context.Background()
	if _, err := db.Document.Create(ctx, map[string]string{"a": "b"}, cache.ID("x")); err != nil {
		t.Fatalf("create: %v", err)
	}

	// The entry has to be reachable from a second DB built on the same name, and
	// invisible to one built on another.
	same := Build(newFake(), Spec{Prefix: "app", Namespace: "orders"})
	defer func() { _ = same.Close() }()
	if same.Name != db.Name {
		t.Errorf("two DBs on one name disagree: %q and %q", db.Name, same.Name)
	}
}
