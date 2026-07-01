package evm

import (
	"errors"
	"testing"

	"github.com/the-protobuf-project/runtime-go/store"
)

func TestRevertToError(t *testing.T) {
	base := errors.New("execution reverted")
	cases := []struct {
		reason string
		want   error // sentinel to errors.Is against, or nil for "wrapped"
	}{
		{"Book: not found", store.ErrNotFound},
		{"Author: already exists", store.ErrAlreadyExists},
		{"Book: not owner", store.ErrPermissionDenied},
		{"Book: not record owner", store.ErrPermissionDenied},
	}
	for _, tc := range cases {
		got := revertToError(base, tc.reason)
		if !errors.Is(got, tc.want) {
			t.Errorf("revertToError(%q) = %v, want errors.Is %v", tc.reason, got, tc.want)
		}
	}

	// An unrecognized reason is reported, not swallowed.
	if got := revertToError(base, "custom failure"); got == nil || !contains(got.Error(), "custom failure") {
		t.Errorf("unrecognized reason: got %v", got)
	}
	// No reason → the original error passes through.
	if got := revertToError(base, ""); got != base {
		t.Errorf("empty reason should pass the original error, got %v", got)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
