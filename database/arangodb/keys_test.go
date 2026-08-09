package arangodb

import "testing"

// The key encoding is the one place where a mistake is silent: a write succeeds
// and every read after it misses, because the stored key and the requested one
// differ only after the server has decoded a URL. So it is checked here rather
// than only through a live round trip.
func TestKeyEncodingRoundTrips(t *testing.T) {
	for _, id := range []string{
		"plain",
		"users/ada",
		"users/a/b/c",
		"v1.2",
		"a/b.c",
		"100%",
		"a%2Fb", // already-encoded text stays text
		"resources/1:cancel",
		"with space",
		"emoji-🙂",
		"",
		".",
		"..",
		"/",
	} {
		enc := escapeKey(id)
		if got := unescapeKey(enc); got != id {
			t.Errorf("%q encoded to %q and came back as %q", id, enc, got)
		}
	}
}

// Whatever comes out has to be safe in both places at once, or the failure is
// the silent one above.
func TestEncodedKeysAreSafeEverywhere(t *testing.T) {
	for _, id := range []string{"users/ada", "100%", "a/b.c", "with space", "emoji-🙂", "a?b#c"} {
		enc := escapeKey(id)
		for i := 0; i < len(enc); i++ {
			c := enc[i]
			if !keySafe(c) && c != keyEscape {
				t.Errorf("%q encoded to %q, which contains an unsafe byte %q", id, enc, c)
			}
		}
	}
}

// Two different ids must never collide, which is what the doubled escape is for.
func TestEncodingIsInjective(t *testing.T) {
	seen := map[string]string{}
	for _, id := range []string{
		"a/b", "a.b", "a..b", "a.2Fb", "a/b/c", "a.b.c", "a%2Fb",
	} {
		enc := escapeKey(id)
		if other, clash := seen[enc]; clash {
			t.Errorf("%q and %q both encode to %q", other, id, enc)
		}
		seen[enc] = id
	}
}

func TestDocumentIDSplits(t *testing.T) {
	id := documentID("users", "users/ada")
	coll, key := splitDocumentID(id)
	if coll != "users" {
		t.Errorf("collection = %q", coll)
	}
	if key != "users/ada" {
		t.Errorf("key = %q, want users/ada", key)
	}
}
