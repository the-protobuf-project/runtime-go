package rabbitmq

import "testing"

// These need no broker: the binding-key rules are pure.
func TestMatches(t *testing.T) {
	cases := []struct {
		name    string
		binding string
		key     string
		want    bool
	}{
		{"identical", "user.created", "user.created", true},
		{"literal mismatch", "user.created", "user.deleted", false},
		{"star takes one word", "user.*", "user.created", true},
		{"star does not span words", "user.*", "user.eu.created", false},
		{"star in the middle", "user.*.created", "user.eu.created", true},
		{"hash takes the tail", "user.#", "user.eu.created", true},
		{"hash takes one word", "user.#", "user.created", true},
		// AMQP's '#' matches zero words as well, unlike NATS's '>'.
		{"hash takes no words", "user.#", "user", true},
		{"bare hash takes everything", "#", "user.created", true},
		{"bare hash takes nothing", "#", "", true},
		// And unlike MQTT's, AMQP's '#' may appear anywhere.
		{"hash in the middle", "user.#.created", "user.eu.west.created", true},
		{"hash in the middle spanning nothing", "user.#.created", "user.created", true},
		{"hash in the middle, no match", "user.#.created", "user.eu.deleted", false},
		{"key longer than binding", "user", "user.created", false},
		{"binding longer than key", "user.created", "user", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := matches(c.binding, c.key); got != c.want {
				t.Errorf("matches(%q, %q) = %v, want %v", c.binding, c.key, got, c.want)
			}
		})
	}
}

func TestHasWildcard(t *testing.T) {
	for _, s := range []string{"user.*", "user.#", "#", "*", "a.#.b"} {
		if !hasWildcard(s) {
			t.Errorf("hasWildcard(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"user.created", "user", "a.b.c"} {
		if hasWildcard(s) {
			t.Errorf("hasWildcard(%q) = true, want false", s)
		}
	}
}

func TestSafeNameStripsRoutingStructure(t *testing.T) {
	for _, s := range []string{"a.b", "a*b", "a#b", "a b"} {
		if got := safeName(s); got == s {
			t.Errorf("safeName(%q) left a reserved character in place", s)
		}
	}
}
