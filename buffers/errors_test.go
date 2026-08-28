package buffers

import (
	"errors"
	"fmt"
	"testing"
)

// errBoom stands in for whatever the encoding returned.
var errBoom = errors.New("cannot set text")

// TestAtBuildsThePathOutward is the property the whole design rests on: no level
// of a conversion knows its own depth, and the path still comes out whole.
func TestAtBuildsThePathOutward(t *testing.T) {
	// As it would unwind: the leaf fails, each caller names its own field.
	err := At("w", errBoom)
	err = At("orientation", err)
	err = At("mount", err)

	if got, want := err.Error(), "mount.orientation.w: cannot set text"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

// TestAtIsNilSafe lets a call site wrap unconditionally, which is what keeps the
// generated code free of an `if err != nil` around every field.
func TestAtIsNilSafe(t *testing.T) {
	if err := At("mount", nil); err != nil {
		t.Errorf("At(nil) = %v, want nil", err)
	}
	if err := Index(2, nil); err != nil {
		t.Errorf("Index(nil) = %v, want nil", err)
	}
}

// TestIndexBindsToTheFieldOnItsLeft covers the composition that a naive join
// gets wrong: an index belongs to the name it indexes, so the separator must not
// appear before it.
func TestIndexBindsToTheFieldOnItsLeft(t *testing.T) {
	err := At("points", Index(3, At("x", errBoom)))

	if got, want := err.Error(), "points[3].x: cannot set text"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

// TestNestedIndexes covers a list of lists, where two indexes meet.
func TestNestedIndexes(t *testing.T) {
	err := At("grid", Index(1, Index(4, errBoom)))

	if got, want := err.Error(), "grid[1][4]: cannot set text"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

// TestUnwrapReachesTheCause keeps errors.Is working through the path wrapper, so
// a caller can still match on what the encoding actually returned.
func TestUnwrapReachesTheCause(t *testing.T) {
	err := At("mount", At("orientation", errBoom))

	if !errors.Is(err, errBoom) {
		t.Error("errors.Is cannot reach the underlying cause")
	}
	var conv *Error
	if !errors.As(err, &conv) {
		t.Fatal("errors.As does not yield a *Error")
	}
	if conv.Path != "mount.orientation" {
		t.Errorf("Path = %q, want mount.orientation", conv.Path)
	}
}

// TestPathlessErrorReadsAsItsCause covers a failure with nothing to attribute:
// the wrapper must not add a bare colon to a message that has no path.
func TestPathlessErrorReadsAsItsCause(t *testing.T) {
	e := &Error{Err: errBoom}
	if got := e.Error(); got != "cannot set text" {
		t.Errorf("Error() = %q, want the bare cause", got)
	}
}

// ExampleAt shows the shape generated code uses.
func ExampleAt() {
	convertField := func() error { return errBoom }

	err := At("mount", At("orientation", At("w", convertField())))
	fmt.Println(err)
	// Output: mount.orientation.w: cannot set text
}
