package nats

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func Test_isIdlePullError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"deadline exceeded (wrapped)", fmtErr("some: %w", context.DeadlineExceeded), true},
		{"deadline substring", errors.New("context deadline exceeded"), true},
		{"timeout substring", errors.New("request timeout while pulling"), true},
		{"no messages", errors.New("no messages available"), true},
		{"expired", errors.New("iterator expired after wait"), true},
		{"heartbeat", errors.New("no heartbeat received"), true},
		{"unrelated", errors.New("some other error"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isIdlePullError(tc.err)
			if got != tc.want {
				t.Fatalf("isIdlePullError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func Test_isConsumerNotFound(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain phrase", errors.New("consumer not found"), true},
		{"api 404 consumer", errors.New("nats: API error: code=404 err_code=10014 description=consumer not found"), true},
		{"err_code only", errors.New("err_code=10014"), true},
		// This is *not* consumer-not-found; should be false.
		{"other 404", errors.New("nats: API error: code=404 err_code=10059 description=stream not found"), false},
		{"unrelated", errors.New("permission denied"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isConsumerNotFound(tc.err)
			if got != tc.want {
				t.Fatalf("isConsumerNotFound(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func Test_isIteratorClosedErr(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"iterator closed canonical", errors.New("nats: messages iterator closed"), true},
		{"mixed case", errors.New("Messages Iterator Closed due to connection"), true},
		{"unrelated", errors.New("some other error"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isIteratorClosedErr(tc.err)
			if got != tc.want {
				t.Fatalf("isIteratorClosedErr(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func Test_isConnClosedErr(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"connection closed canonical", errors.New("nats: connection closed"), true},
		{"mixed case", errors.New("Connection Closed by server"), true},
		{"unrelated", errors.New("dial tcp: no route to host"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isConnClosedErr(tc.err)
			if got != tc.want {
				t.Fatalf("isConnClosedErr(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// small helper to wrap errors with formatting (avoids importing fmt just for Sprintf)
func fmtErr(format string, wrap error) error {
	// minimal inline formatter to keep deps tiny in tests
	// format is expected to contain one %w; we emulate it simply.
	return errors.New(strings.Replace(format, "%w", wrap.Error(), 1))
}
