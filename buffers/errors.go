package buffers

// errors.go answers the question a failed conversion has to answer: which field.
//
// The encodings underneath report failures in their own terms, and those terms
// name nothing a caller can act on. capnp returns "no such pointer" and
// flatbuffers panics on a builder used out of order; neither knows it was in the
// middle of a Sensor's mount pose. A conversion walks a tree, so the useful
// message is the path it was at, and the only place that path exists is spread
// across the stack of calls that were unwinding.
//
// So it is accumulated on the way out. Each level names the one segment it knows
// and hands the error up, and the full path assembles itself without any level
// having to know where it sits.

import (
	"errors"
	"strconv"
	"strings"
)

// Error is a conversion failure, carrying the field path it happened at.
type Error struct {
	// Path is the dotted route from the message being converted to the field
	// that failed, e.g. "mount.orientation.w" or "points[3].x".
	Path string

	// Err is the underlying failure, as the encoding reported it.
	Err error
}

// Error renders the path and the cause.
func (e *Error) Error() string {
	if e.Path == "" {
		return e.Err.Error()
	}
	return e.Path + ": " + e.Err.Error()
}

// Unwrap exposes the cause to errors.Is and errors.As, so a caller can still
// match on whatever the encoding returned.
func (e *Error) Unwrap() error { return e.Err }

// At attaches a field name to a failing conversion, building the path outward as
// the error unwinds.
//
// A nil error stays nil, so a call site can wrap unconditionally:
//
//	return buffers.At("mount", copyPose(src.GetMount(), dst))
//
// Nesting composes without any level knowing its own depth: the innermost call
// records "w", its caller prepends "orientation", and the result is
// "mount.orientation.w".
func At(field string, err error) error {
	if err == nil {
		return nil
	}
	var conv *Error
	if errors.As(err, &conv) {
		conv.Path = join(field, conv.Path)
		return conv
	}
	return &Error{Path: field, Err: err}
}

// Index attaches a repeated field's position, so a failure in one element of a
// long list names the element rather than the list.
//
// It is separate from [At] because the two compose differently: an index binds
// to the segment on its left, `points[3]`, where a field name starts a new one.
func Index(i int, err error) error {
	if err == nil {
		return nil
	}
	suffix := "[" + strconv.Itoa(i) + "]"

	var conv *Error
	if errors.As(err, &conv) {
		// Bind to the enclosing field rather than standing alone: the caller
		// wraps this in At("points", …), which must produce "points[3].x" and
		// not "points.[3].x".
		conv.Path = suffix + dotted(conv.Path)
		return conv
	}
	return &Error{Path: suffix, Err: err}
}

// join links two path segments, tolerating either being empty and keeping an
// index bound to the name it indexes.
func join(outer, inner string) string {
	switch {
	case outer == "":
		return inner
	case inner == "":
		return outer
	case strings.HasPrefix(inner, "["):
		return outer + inner
	}
	return outer + "." + inner
}

// dotted prefixes a path with a separator unless it is empty or already an
// index.
func dotted(path string) string {
	switch {
	case path == "":
		return ""
	case strings.HasPrefix(path, "["):
		return path
	}
	return "." + path
}
