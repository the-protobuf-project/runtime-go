package streams

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrInvalidHeader is returned when a header breaks the rules every broker
// here can carry: an empty or oversized key, a control character in a value,
// a prefix the broker reserves.
//
// It is a settled answer, like [ErrUnsupported]: the same header is refused
// on every attempt, so [WithPublisherRetry] gives up on it immediately.
var ErrInvalidHeader = errors.New("streams: invalid header")

const (
	// MaxHeaderKey is the longest header key accepted, in bytes.
	MaxHeaderKey = 256

	// MaxHeaderValue is the longest header value accepted, in bytes.
	MaxHeaderValue = 4096

	// MaxHeaderTotal is the most header bytes, keys and values together, one
	// message may carry.
	//
	// Small enough for every broker's own limit, MQTT's user properties and
	// NATS's header block among them, so a header accepted here is not refused
	// later by the broker.
	MaxHeaderTotal = 16 * 1024
)

// Headers are carried with a message, outside its payload.
//
// A plain string-to-string map with no schema and no keys this package
// reserves: whatever a deployment wants to say about a message travels here,
// the language that produced it (lang), a trace (traceparent), a content type,
// a site. Keys are case-sensitive, and encode in key order, so the same
// headers always encode to the same bytes.
//
// A [Stream] carries default headers stamped on everything published through
// it; [Header] and [WithHeaders] lay a publish's own on top, a key set there
// winning. Each provider sends them with its broker's own mechanism (Kafka
// record headers, NATS headers, AMQP properties, MQTT 5 user properties), so
// other tools on the broker see them too.
type Headers map[string]string

// Get returns key's value, or "" when it is not set.
func (h Headers) Get(key string) string {
	return h[key]
}

// Keys returns the keys in order, the order headers encode in.
func (h Headers) Keys() []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Clone returns a copy that can be changed without changing h, or nil when h
// is empty.
func (h Headers) Clone() Headers {
	if len(h) == 0 {
		return nil
	}
	out := make(Headers, len(h))
	for k, v := range h {
		out[k] = v
	}
	return out
}

// Merged returns h with over laid on top: a key in both takes over's value.
// This is how a stream's defaults meet a publish's own. Neither is changed.
func (h Headers) Merged(over Headers) Headers {
	if len(over) == 0 {
		return h.Clone()
	}
	out := make(Headers, len(h)+len(over))
	for k, v := range h {
		out[k] = v
	}
	for k, v := range over {
		out[k] = v
	}
	return out
}

// Validate checks the headers fit every broker this package speaks to.
//
// A key is non-empty, at most [MaxHeaderKey] bytes, and made of visible ASCII
// other than ':', which the line-based formats (NATS) use as the separator. A
// value is at most [MaxHeaderValue] bytes with no line breaks or other control
// characters. All of it together is at most [MaxHeaderTotal]. The error names
// the offending header, so it fails at the call that set it rather than at a
// broker later.
func (h Headers) Validate() error {
	total := 0
	for _, key := range h.Keys() {
		value := h[key]
		if key == "" {
			return invalidHeader("a header key is empty")
		}
		if len(key) > MaxHeaderKey {
			return invalidHeader("header key %q… is %d bytes, beyond the %d-byte limit", key[:32], len(key), MaxHeaderKey)
		}
		for _, c := range key {
			if c <= ' ' || c > '~' || c == ':' {
				return invalidHeader("header key %q contains %q; keys are visible ASCII without ':'", key, c)
			}
		}
		if len(value) > MaxHeaderValue {
			return invalidHeader("header %q has a %d-byte value, beyond the %d-byte limit", key, len(value), MaxHeaderValue)
		}
		if strings.IndexFunc(value, isControl) >= 0 {
			return invalidHeader("header %q contains a control character; values are one line of text", key)
		}
		total += len(key) + len(value)
	}
	if total > MaxHeaderTotal {
		return invalidHeader("headers total %d bytes, beyond the %d-byte limit", total, MaxHeaderTotal)
	}
	return nil
}

// RejectReserved refuses any key starting with one of prefixes, which a
// provider's broker reserves for itself (NATS's "Nats-", for one). Setting one
// would silently change what the broker does with the message.
func (h Headers) RejectReserved(backend string, prefixes ...string) error {
	for _, key := range h.Keys() {
		for _, p := range prefixes {
			if strings.HasPrefix(key, p) {
				return invalidHeader("header key %q uses the %q prefix %s reserves", key, p, backend)
			}
		}
	}
	return nil
}

// isControl reports the C0 and C1 control characters and DEL.
func isControl(r rune) bool {
	return r < 0x20 || (r >= 0x7f && r < 0xa0)
}

// invalidHeader wraps [ErrInvalidHeader] with the reason, so callers test
// with errors.Is.
func invalidHeader(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidHeader, fmt.Sprintf(format, args...))
}
