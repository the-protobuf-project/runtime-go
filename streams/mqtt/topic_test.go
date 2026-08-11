package mqtt

import "testing"

func TestMatches(t *testing.T) {
	cases := []struct {
		name   string
		filter string
		topic  string
		want   bool
	}{
		{"identical", "user/created", "user/created", true},
		{"literal mismatch", "user/created", "user/deleted", false},
		{"plus takes one level", "user/+", "user/created", true},
		{"plus does not span levels", "user/+", "user/eu/created", false},
		{"plus in the middle", "user/+/created", "user/eu/created", true},
		{"hash takes the tail", "user/#", "user/eu/created", true},
		{"hash takes one level", "user/#", "user/created", true},
		// Where MQTT differs from NATS: '#' matches the parent level too, so
		// this is true where the equivalent NATS '>' would be false.
		{"hash matches the parent level", "user/#", "user", true},
		{"bare hash takes everything", "#", "user/created", true},
		{"hash is only trailing", "a/#/b", "a/x/b", false},
		{"topic longer than filter", "user", "user/created", false},
		{"filter longer than topic", "user/created", "user", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := matches(c.filter, c.topic); got != c.want {
				t.Errorf("matches(%q, %q) = %v, want %v", c.filter, c.topic, got, c.want)
			}
		})
	}
}

func TestHasWildcard(t *testing.T) {
	for _, s := range []string{"user/+", "user/#", "#", "+"} {
		if !hasWildcard(s) {
			t.Errorf("hasWildcard(%q) = false, want true", s)
		}
	}
	if hasWildcard("user/created") {
		t.Error("hasWildcard flagged a concrete topic")
	}
}

// The client id is the session, so it has to be the same on every attach or a
// restart is a stranger the broker kept nothing for.
func TestConsumerIDIsStableAndPerSubject(t *testing.T) {
	first := consumerID("billing", "user/created")
	again := consumerID("billing", "user/created")
	if first != again {
		t.Errorf("consumerID is not stable: %q then %q", first, again)
	}
	if consumerID("billing", "user/created") == consumerID("billing", "user/deleted") {
		t.Error("consumerID does not distinguish subjects")
	}
	if consumerID("billing", "user/created") == consumerID("shipping", "user/created") {
		t.Error("consumerID does not distinguish consumers")
	}
}

func TestSafeNameStripsTopicStructure(t *testing.T) {
	for _, s := range []string{"a/b", "a+b", "a#b"} {
		if got := safeName(s); got == s {
			t.Errorf("safeName(%q) left a reserved character in place", s)
		}
	}
}
