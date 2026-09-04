package rfc

// validate.go runs the buf.validate rules the schemas carry.
//
// # Why this is a link in the chain rather than a function
//
// The rules are annotations on the protos — a PREF between 0 and 100, a resource
// name matching `^vcards/[^/]+$`, and the CEL relations a field constraint
// cannot express, like "a DISPLAY alarm requires a description". Nothing runs
// them unless something asks, and until this package there was no obvious place
// to ask from: a caller parsing a document holds bytes, not a message, so the
// thing to validate does not exist yet at the moment they would want to say so.
//
// That is the whole argument for `Validate` being chainable rather than a
// standalone call. It marks the intent, and the terminal method runs the check
// at the only point where there is something to check:
//
//	card, err := rfc.VCard(text).Validate().Card()
//
// reads as "parse this, reject it if the model is invalid, then convert" — and
// the parse has to happen first for the sentence to mean anything.
//
// # What it does and does not cover
//
// Message-scoped rules only: ranges, patterns, required fields, enum
// membership, and CEL relations between fields of one message. Anything needing
// state a message does not carry — is this uid unique, does this parent exist —
// is the server's job and stays there.

import (
	"fmt"
	"sync"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"
)

// validator is built once and shared.
//
// protovalidate.New compiles every CEL expression in the descriptors it can
// reach, which is far too expensive to repeat per call. sync.OnceValues also
// keeps the failure: if the rules do not compile, every Validate reports the
// same error rather than retrying a build that cannot succeed.
var validator = sync.OnceValues(func() (protovalidate.Validator, error) {
	// Wrapped rather than passed directly: protovalidate.New is variadic, so
	// its signature does not unify with sync.OnceValues' func() (T1, T2).
	return protovalidate.New()
})

// ValidationError is a message that failed its own buf.validate rules.
type ValidationError struct {
	// Subject names what was validated: "contact", "card", "event".
	Subject string

	// Err is protovalidate's report, listing each violated rule.
	Err error
}

// Error renders the subject and the violations.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("rfc: %s is not valid: %v", e.Subject, e.Err)
}

// Unwrap exposes protovalidate's error, so a caller can reach the individual
// violations through errors.As.
func (e *ValidationError) Unwrap() error { return e.Err }

// check runs the rules against a message, naming the subject on failure.
//
// A nil message passes: an absent value is the caller's business, and a
// conversion that produced nothing has already reported why.
func check(subject string, m proto.Message) error {
	if m == nil {
		return nil
	}
	v, err := validator()
	if err != nil {
		return fmt.Errorf("rfc: build validator: %w", err)
	}
	if err := v.Validate(m); err != nil {
		return &ValidationError{Subject: subject, Err: err}
	}
	return nil
}
