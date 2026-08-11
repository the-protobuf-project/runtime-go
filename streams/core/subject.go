package core

import (
	"fmt"
	"slices"
	"strings"

	"github.com/the-protobuf-project/runtime-go/streams"
)

// Matches reports whether subject satisfies pattern under NATS token wildcards.
//
// A subject is dot-separated tokens. Within a pattern, `*` stands for exactly
// one token and `>` for one or more trailing tokens, so `orders.*` matches
// `orders.placed` but not `orders.us.placed`, and `orders.>` matches both.
//
// `>` is only a wildcard as the final token, which is how NATS reads it: a
// pattern with `>` in the middle has a literal token that no subject with a `>`
// in it will be published under, so it simply matches nothing.
func Matches(pattern, subject string) bool {
	if pattern == subject {
		return true
	}

	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")

	for i, token := range p {
		switch token {
		case ">":
			// Trailing, and there has to be at least one token left to consume:
			// `a.>` does not match a bare `a`.
			return i == len(p)-1 && len(s) > i
		case "*":
			if i >= len(s) {
				return false
			}
		default:
			if i >= len(s) || s[i] != token {
				return false
			}
		}
	}
	return len(p) == len(s)
}

// Declares reports whether subject is exactly one of declared.
//
// This is the rule for a provider whose subjects are literal names — a Redis
// pub/sub channel is a string, not a pattern, so a stream declaring `orders.*`
// has a channel by that name and nothing else.
func Declares(declared []string, subject string) bool {
	return slices.Contains(declared, subject)
}

// DeclaresPattern reports whether any of declared matches subject as a pattern.
//
// This is the rule for a provider whose subjects are patterns. It is separate
// from [Declares] because reading a stream's subjects one way when the backend
// reads them the other is how a publish succeeds against a subject nothing is
// listening to.
func DeclaresPattern(declared []string, subject string) bool {
	for _, d := range declared {
		if Matches(d, subject) {
			return true
		}
	}
	return false
}

// ErrSubject builds the error a provider returns for a subject its stream does
// not declare, naming what was asked for and what is on offer.
func ErrSubject(streamID, subject string, declared []string) error {
	return fmt.Errorf("%w: %q (stream %s declares %v)",
		streams.ErrUnknownSubject, subject, streamID, declared)
}
