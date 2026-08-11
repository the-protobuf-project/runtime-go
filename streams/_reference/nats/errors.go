package nats

import (
	"context"
	"errors"
	"strings"
)

// IsIdlePullError treats transient/no-message conditions as non-fatal pulls.
// Adjust this as needed for your SDK version.
func isIdlePullError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "deadline"),
		strings.Contains(msg, "timeout"),
		strings.Contains(msg, "no messages"),
		strings.Contains(msg, "expired"),
		strings.Contains(strings.ToLower(msg), "heartbeat"),
		errors.Is(err, context.DeadlineExceeded):
		return true
	default:
		return false
	}
}

// isConsumerNotFound reports whether err indicates that a consumer was not found.
// (NATS/JetStream errors may vary; match loosely.)
func isConsumerNotFound(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "consumer not found") || // plain phrase
		strings.Contains(s, "err_code=10014") || // consumer-not-found err_code
		(strings.Contains(s, "code=404") && strings.Contains(s, "consumer"))
}

// isIteratorClosedErr reports whether err indicates that a messages iterator was closed.
func isIteratorClosedErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "messages iterator closed")
}

// isConnClosedErr reports whether err indicates that the NATS connection was closed.
func isConnClosedErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "connection closed")
}
