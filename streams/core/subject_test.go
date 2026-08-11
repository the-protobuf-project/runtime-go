package core

import (
	"errors"
	"strings"
	"testing"

	"github.com/the-protobuf-project/runtime-go/streams"
)

func TestMatches(t *testing.T) {
	cases := []struct {
		name    string
		pattern string
		subject string
		want    bool
	}{
		{"identical", "orders.placed", "orders.placed", true},
		{"literal mismatch", "orders.placed", "orders.shipped", false},
		{"star takes one token", "orders.*", "orders.placed", true},
		{"star does not span tokens", "orders.*", "orders.us.placed", false},
		{"star in the middle", "orders.*.placed", "orders.us.placed", true},
		{"star in the middle spans nothing", "orders.*.placed", "orders.us.eu.placed", false},
		{"gt takes the tail", "orders.>", "orders.us.placed", true},
		{"gt takes one token", "orders.>", "orders.placed", true},
		{"gt needs at least one token", "orders.>", "orders", false},
		{"bare gt takes everything", ">", "orders.us.placed", true},
		{"bare gt takes one", ">", "orders", true},
		{"bare star takes one", "*", "orders", true},
		{"bare star takes only one", "*", "orders.placed", false},
		{"gt is only trailing", "a.>.b", "a.x.b", false},
		{"subject longer than pattern", "orders", "orders.placed", false},
		{"pattern longer than subject", "orders.placed", "orders", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Matches(c.pattern, c.subject); got != c.want {
				t.Errorf("Matches(%q, %q) = %v, want %v", c.pattern, c.subject, got, c.want)
			}
		})
	}
}

func TestDeclaresIsExact(t *testing.T) {
	declared := []string{"orders.*", "orders.placed"}

	if !Declares(declared, "orders.*") {
		t.Error("Declares did not find the literal subject it was given")
	}
	// The pattern is a name here, not a rule: a provider whose channels are
	// literal strings has one called "orders.*" and none called "orders.us".
	if Declares(declared, "orders.us") {
		t.Error("Declares treated a declared pattern as a wildcard")
	}
}

func TestDeclaresPatternReadsSubjectsAsPatterns(t *testing.T) {
	declared := []string{"orders.*", "events.>"}

	for _, subject := range []string{"orders.placed", "events.us.signup"} {
		if !DeclaresPattern(declared, subject) {
			t.Errorf("DeclaresPattern(%v, %q) = false, want true", declared, subject)
		}
	}
	if DeclaresPattern(declared, "orders.us.placed") {
		t.Error("DeclaresPattern let a two-token subject through a one-token wildcard")
	}
}

func TestErrSubjectWrapsTheSentinel(t *testing.T) {
	err := ErrSubject("s-1", "orders.shipped", []string{"orders.placed"})

	if !errors.Is(err, streams.ErrUnknownSubject) {
		t.Fatalf("ErrSubject does not wrap ErrUnknownSubject: %v", err)
	}
	// The message has to name what was asked for and what is on offer, or the
	// caller is left rereading the stream declaration to find a typo.
	for _, want := range []string{"orders.shipped", "orders.placed", "s-1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}
