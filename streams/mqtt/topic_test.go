package mqtt

import (
	"math"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/the-protobuf-project/runtime-go/streams"
)

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

// runtime-rs derives the same id, so both runtimes resume the same session.
func TestConsumerIDMatchesRust(t *testing.T) {
	// fnv.New32a over "" is the offset basis; over "a" it is 0xe40c292c.
	if got := consumerID("c", ""); got != "c-811c9dc5" {
		t.Errorf("consumerID(c, \"\") = %q", got)
	}
	if got := consumerID("a/b", "a"); got != "a-b-e40c292c" {
		t.Errorf("consumerID(a/b, a) = %q", got)
	}
}

func TestExpiryRoundsUpToWholeSeconds(t *testing.T) {
	for _, tc := range []struct {
		in   time.Duration
		want uint32
	}{
		{time.Millisecond, 1},
		{999 * time.Millisecond, 1},
		{time.Second, 1},
		{1001 * time.Millisecond, 2},
		{90 * time.Second, 90},
		{time.Duration(math.MaxInt64), math.MaxUint32},
	} {
		if got := expiryInterval(tc.in); got != tc.want {
			t.Errorf("expiryInterval(%s) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestPropertiesCarryHeadersAndExpiryOnlyWhenSet(t *testing.T) {
	if p := publishProperties(nil, 0); p != nil {
		t.Errorf("properties for nothing = %+v, want none", p)
	}
	headers := streams.Headers{"site": "plant-3", "lang": "go"}
	p := publishProperties(headers, 1500*time.Millisecond)
	want := paho.UserProperties{{Key: "lang", Value: "go"}, {Key: "site", Value: "plant-3"}}
	if len(p.User) != len(want) || p.User[0] != want[0] || p.User[1] != want[1] {
		t.Errorf("user properties = %v, want %v in key order", p.User, want)
	}
	if p.MessageExpiry == nil || *p.MessageExpiry != 2 {
		t.Errorf("expiry = %v, want 2", p.MessageExpiry)
	}
	if p.ContentType != "" {
		t.Errorf("content type = %q, want none", p.ContentType)
	}
	p = publishProperties(nil, 5*time.Second)
	if len(p.User) != 0 || *p.MessageExpiry != 5 {
		t.Errorf("expiry-only properties = %+v", p)
	}

	if h := headersOf(&paho.Publish{Properties: p}); h != nil {
		t.Errorf("headers of an expiry-only publish = %v", h)
	}
	if h := headersOf(&paho.Publish{Properties: publishProperties(headers, 0)}); len(h) != 2 || h.Get("lang") != "go" {
		t.Errorf("headers round trip = %v", h)
	}
	if h := headersOf(&paho.Publish{}); h != nil {
		t.Errorf("headers of a bare publish = %v", h)
	}
}

func TestARepeatedUserPropertyKeepsTheLast(t *testing.T) {
	p := &paho.Publish{Properties: &paho.PublishProperties{User: paho.UserProperties{{Key: "k", Value: "1"}, {Key: "k", Value: "2"}}}}
	if got := headersOf(p).Get("k"); got != "2" {
		t.Errorf("repeated key = %q, want the last", got)
	}
}

func TestATopicAlwaysTakesTheSameConnection(t *testing.T) {
	if slot("any", 1) != 0 || slot("any", 0) != 0 {
		t.Error("a single connection is not always slot 0")
	}
	used := map[int]bool{}
	for n := range 64 {
		topic := "plant/" + string(rune('a'+n%26)) + "/" + time.Duration(n).String() + "/temp"
		if first, again := slot(topic, 4), slot(topic, 4); first != again {
			t.Errorf("%s moved between connections", topic)
		}
		if s := slot(topic, 4); s < 0 || s >= 4 {
			t.Errorf("%s took slot %d of 4", topic, s)
		}
		used[slot(topic, 4)] = true
	}
	if len(used) != 4 {
		t.Errorf("topics spread over %d of 4 connections", len(used))
	}
}
