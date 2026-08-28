package buffers

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestTimestampRoundTrips is the ordinary path: out to two integers and back.
func TestTimestampRoundTrips(t *testing.T) {
	want := timestamppb.New(time.Date(2026, 8, 28, 14, 30, 45, 123456789, time.UTC))

	sec, nsec := TimestampParts(want)
	got, err := Timestamp(sec, nsec)
	if err != nil {
		t.Fatalf("Timestamp: %v", err)
	}
	if got.GetSeconds() != want.GetSeconds() || got.GetNanos() != want.GetNanos() {
		t.Errorf("round trip = %v, want %v", got, want)
	}
}

// TestInvalidTimestampIsRejected covers what a target cannot enforce.
//
// Nothing in a capnp struct or a FlatBuffers vtable constrains the nanos slot, so
// a producer can put anything in it. Building the protobuf value regardless would
// move the failure to whoever calls CheckValid next, usually far from here.
func TestInvalidTimestampIsRejected(t *testing.T) {
	for _, c := range []struct {
		name string
		sec  int64
		nsec int32
	}{
		{"nanos above a second", 0, 1_000_000_000},
		{"negative nanos", 0, -1},
		{"seconds far past the supported range", 1 << 62, 0},
	} {
		if _, err := Timestamp(c.sec, c.nsec); err == nil {
			t.Errorf("%s: accepted seconds=%d nanos=%d", c.name, c.sec, c.nsec)
		}
	}
}

// TestDurationRoundTripsIncludingNegative covers the sign rule that catches
// people out: a negative span carries the sign on both fields.
func TestDurationRoundTripsIncludingNegative(t *testing.T) {
	want := durationpb.New(-1500 * time.Millisecond)

	sec, nsec := DurationParts(want)
	if sec > 0 || nsec > 0 {
		t.Fatalf("a negative duration split to seconds=%d nanos=%d; both should be <= 0", sec, nsec)
	}
	got, err := Duration(sec, nsec)
	if err != nil {
		t.Fatalf("Duration: %v", err)
	}
	if got.AsDuration() != -1500*time.Millisecond {
		t.Errorf("round trip = %v, want -1.5s", got.AsDuration())
	}
}

// TestMismatchedDurationSignIsRejected is the pair a target can produce by
// storing the two numbers independently, and that protobuf considers invalid.
func TestMismatchedDurationSignIsRejected(t *testing.T) {
	if _, err := Duration(-1, 500_000_000); err == nil {
		t.Error("accepted seconds=-1 nanos=+500000000, which is not a valid Duration")
	}
}

// TestNilInputsYieldZeroParts checks the absent case does not panic. Whether an
// absent field should be written at all is the caller's decision, made from the
// target's own has-check.
func TestNilInputsYieldZeroParts(t *testing.T) {
	if sec, nsec := TimestampParts(nil); sec != 0 || nsec != 0 {
		t.Errorf("TimestampParts(nil) = %d, %d, want 0, 0", sec, nsec)
	}
	if sec, nsec := DurationParts(nil); sec != 0 || nsec != 0 {
		t.Errorf("DurationParts(nil) = %d, %d, want 0, 0", sec, nsec)
	}
}

// TestFieldMaskKeepsAbsentApartFromEmpty is the distinction AIP-134 leans on: an
// absent update_mask means replace everything, an empty one does not.
func TestFieldMaskKeepsAbsentApartFromEmpty(t *testing.T) {
	if got := FieldMaskPaths(nil); got != nil {
		t.Errorf("FieldMaskPaths(nil) = %v, want nil", got)
	}
	if got := FieldMask(nil); got != nil {
		t.Errorf("FieldMask(nil) = %v, want nil", got)
	}

	empty := FieldMask([]string{})
	if empty == nil {
		t.Fatal("FieldMask([]) collapsed an empty mask to absent")
	}
	if len(empty.GetPaths()) != 0 {
		t.Errorf("FieldMask([]) = %v, want an empty mask", empty)
	}

	full := &fieldmaskpb.FieldMask{Paths: []string{"display_name", "mount"}}
	if got := FieldMask(FieldMaskPaths(full)); len(got.GetPaths()) != 2 {
		t.Errorf("round trip = %v, want two paths", got)
	}
}
