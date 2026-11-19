package servicex

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/the-protobuf-project/runtime-go/grpc/problem"
)

// The repositories and the servers must agree on each outcome, or a
// repository's not found would reach the client as UNKNOWN.
func TestSentinelsAreProblems(t *testing.T) {
	for _, pair := range [][2]error{
		{ErrNotFound, problem.ErrNotFound},
		{ErrAlreadyExists, problem.ErrAlreadyExists},
		{ErrConflict, problem.ErrConflict},
		{ErrInvalidArgument, problem.ErrInvalidArgument},
	} {
		if !errors.Is(pair[0], pair[1]) {
			t.Errorf("servicex %v is not problem's", pair[0])
		}
	}
}

func TestTranslatorCodes(t *testing.T) {
	tr := NewTranslator("example.com")
	for _, c := range []struct {
		err  error
		code codes.Code
	}{
		{fmt.Errorf("shelf: %w", ErrNotFound), codes.NotFound},
		{fmt.Errorf("shelf: %w", ErrAlreadyExists), codes.AlreadyExists},
		{fmt.Errorf("shelf: %w", ErrConflict), codes.Aborted},
		{fmt.Errorf("shelf: %w", ErrInvalidArgument), codes.InvalidArgument},
		{fmt.Errorf("shelf: %w", ErrFailedPrecondition), codes.FailedPrecondition},
	} {
		if got := status.Code(tr.ToStatus(c.err)); got != c.code {
			t.Errorf("%v: code %v, want %v", c.err, got, c.code)
		}
	}
}

func TestIsNotFound(t *testing.T) {
	if !IsNotFound(fmt.Errorf("get: %w", ErrNotFound)) {
		t.Error("a wrapped sentinel is not found")
	}
	if !IsNotFound(status.Error(codes.NotFound, "gone")) {
		t.Error("a NOT_FOUND status is not found")
	}
	if IsNotFound(errors.New("boom")) {
		t.Error("an unrelated error is not not found")
	}
}

func TestNotFoundAs(t *testing.T) {
	err := NotFoundAs(ErrNotFound, "library.example.com/Shelf", "shelves/a")
	var p *problem.Problem
	if !errors.As(err, &p) || p.Reason != ReasonNotFound || p.ResourceName != "shelves/a" {
		t.Fatalf("NotFoundAs = %#v", err)
	}
	if other := errors.New("boom"); !errors.Is(NotFoundAs(other, "x", "y"), other) {
		t.Error("an error that is not not found passes through unchanged")
	}
}

func TestCheckID(t *testing.T) {
	for id, ok := range map[string]bool{
		"":          true,
		"shelf-1":   true,
		"1shelf":    false,
		"Shelf":     false,
		"shelf-":    false,
		"a":         true,
		"has_under": false,
	} {
		if got := CheckID("shelf_id", id) == nil; got != ok {
			t.Errorf("CheckID(%q) ok = %v, want %v", id, got, ok)
		}
	}
}

func TestCheckName(t *testing.T) {
	cases := []struct {
		name     string
		wildcard bool
		ok       bool
	}{
		{"shelves/a/books/b", false, true},
		{"shelves/-/books/b", false, false},
		{"shelves/-/books/b", true, true},
		{"shelves/a", false, false},
		{"racks/a/books/b", false, false},
		{"", false, false},
	}
	for _, c := range cases {
		err := CheckName("name", c.name, c.wildcard, "shelves", "books")
		if (err == nil) != c.ok {
			t.Errorf("CheckName(%q, wildcard=%v) = %v", c.name, c.wildcard, err)
		}
	}
}

func TestTouchesAndSelected(t *testing.T) {
	paths := []string{"title", "author.name"}
	for field, want := range map[string]bool{
		"title":        true,
		"author":       true,
		"author.name":  true,
		"author.email": false,
		"isbn":         false,
	} {
		if got := Touches(paths, field); got != want {
			t.Errorf("Touches(%v, %q) = %v", paths, field, got)
		}
	}
	if !Touches(nil, "anything") {
		t.Error("an empty mask touches every field")
	}
	if got := Selected(paths, []string{"isbn", "title"}); len(got) != 1 || got[0] != "title" {
		t.Errorf("Selected = %v", got)
	}
}

func TestMask(t *testing.T) {
	if got := Mask(nil).GetPaths(); len(got) != 1 || got[0] != "*" {
		t.Errorf("Mask(nil) = %v", got)
	}
	in := []string{"title"}
	m := Mask(in)
	in[0] = "changed"
	if m.GetPaths()[0] != "title" {
		t.Error("Mask must not alias its input")
	}
}
