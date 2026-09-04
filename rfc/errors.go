package rfc

// errors.go answers the question a failed conversion has to answer: which
// conversion.
//
// The codecs underneath report failures in the vocabulary of the document they
// were reading — "DURATION \"P1X\" has unknown unit 'X'", "jcal component must
// be a 3-element array". Those are the right words for the failure and the
// wrong words for the caller, who ran `rfc.JCal(b).Event()` and needs to know
// which of the two steps that expression hides was the one that broke.
//
// So a conversion names itself on the way out. The step is known statically at
// every call site, which is why this is a wrapper rather than anything that has
// to be threaded through the codecs.

import (
	"errors"
	"fmt"
)

// Error is a conversion failure, carrying the conversion it happened in.
type Error struct {
	// From names what was being converted, as the caller supplied it:
	// "vcard", "jcal", "contact".
	From string

	// To names what was wanted: "contact", "event", "card".
	To string

	// Err is the underlying failure, as the codec reported it.
	Err error
}

// Error renders the conversion and the cause.
func (e *Error) Error() string {
	return fmt.Sprintf("rfc: %s to %s: %v", e.From, e.To, e.Err)
}

// Unwrap exposes the cause to errors.Is and errors.As, so a caller can still
// match on whatever the codec returned.
func (e *Error) Unwrap() error { return e.Err }

// fail wraps a codec error with the conversion it came from.
//
// A nil error stays nil, so a call site can wrap unconditionally and stay a
// single expression:
//
//	return c, fail("vcard", "contact", err)
//
// A conversion that runs two steps — decode, then convert — reports the step
// that actually failed rather than the whole expression, because each step
// wraps its own call.
func fail(from, to string, err error) error {
	if err == nil {
		return nil
	}
	// Already named by an inner step: a two-link chain reports where it
	// broke, not where it was called.
	var conv *Error
	if errors.As(err, &conv) {
		return conv
	}
	return &Error{From: from, To: to, Err: err}
}
