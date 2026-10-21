package grpc

import "testing"

func TestPatternTemplate(t *testing.T) {
	for re, want := range map[string]string{
		`^organizationalUnits/[^/]+$`:       "organizationalUnits/{id}",
		`^calendars/[^/]+/events/[^/]+$`:    "calendars/{id}/events/{id}",
		`^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$`: "",
		`^-?[0-9]+(\.[0-9]+)?s$`:            "",
	} {
		if got := patternTemplate(re); got != want {
			t.Errorf("patternTemplate(%q) = %q, want %q", re, got, want)
		}
	}
}

func TestJoinField(t *testing.T) {
	for _, tc := range []struct{ field, desc, want string }{
		{"window", "is required", "window is required"},
		{"parent", "must look like organizationalUnits/{id}; got \"x\"", "parent must look like organizationalUnits/{id}; got \"x\""},
		{"page_size", "value must be greater than or equal to 0", "page_size: value must be greater than or equal to 0"},
	} {
		if got := joinField(tc.field, tc.desc); got != tc.want {
			t.Errorf("joinField(%q, %q) = %q, want %q", tc.field, tc.desc, got, tc.want)
		}
	}
}
