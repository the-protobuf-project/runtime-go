package buffers

// wellknown.go converts the google.protobuf types that no target encoding has.
//
// All three targets render a Timestamp and a Duration the same way, as a pair of
// a signed 64-bit seconds and a signed 32-bit nanos, because that is exactly what
// the proto definitions are — see the preludes protoc-gen-buffers emits. So the
// conversion is the same two integers whichever encoding is on the other side,
// which is what makes it belong here rather than in generated code.
//
// # Presence is the caller's business
//
// An absent Timestamp and one set to the Unix epoch are different in protobuf and
// identical once flattened to two zeroes. This package will not guess between
// them: the parts functions take and return the numbers, and generated code asks
// the *target* whether the field was set — capnp has HasFoo, FlatBuffers gives a
// null table field — before deciding whether to build a message at all.
//
// # Validation happens on the way in
//
// protobuf constrains both types: nanos must fall in [0, 999999999] and seconds
// in a range around the epoch. Nothing in a FlatBuffers vtable or a capnp struct
// enforces that, so a buggy or hostile producer can put 2^31-1 in the nanos slot.
// Building the protobuf value without checking would defer the failure to
// whatever eventually calls CheckValid — usually far away, and usually reported
// as a bad request rather than as a bad conversion.

import (
	"fmt"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TimestampParts splits a Timestamp into the seconds and nanos every target
// stores. A nil Timestamp yields two zeroes, which the caller should have
// declined to write at all — see this file's note on presence.
func TimestampParts(ts *timestamppb.Timestamp) (seconds int64, nanos int32) {
	if ts == nil {
		return 0, 0
	}
	return ts.GetSeconds(), ts.GetNanos()
}

// Timestamp rebuilds a Timestamp from the parts a target stored, rejecting a
// pair protobuf would consider invalid.
func Timestamp(seconds int64, nanos int32) (*timestamppb.Timestamp, error) {
	ts := &timestamppb.Timestamp{Seconds: seconds, Nanos: nanos}
	if err := ts.CheckValid(); err != nil {
		return nil, fmt.Errorf("timestamp seconds=%d nanos=%d is not a valid google.protobuf.Timestamp: %w",
			seconds, nanos, err)
	}
	return ts, nil
}

// DurationParts splits a Duration into the seconds and nanos every target
// stores.
func DurationParts(d *durationpb.Duration) (seconds int64, nanos int32) {
	if d == nil {
		return 0, 0
	}
	return d.GetSeconds(), d.GetNanos()
}

// Duration rebuilds a Duration from the parts a target stored, rejecting a pair
// protobuf would consider invalid.
//
// Duration's rule is the one people are surprised by: a negative span carries the
// sign on *both* fields, so seconds=-1 nanos=+500000000 is not -0.5s, it is
// invalid. A target that stored the two independently can produce it.
func Duration(seconds int64, nanos int32) (*durationpb.Duration, error) {
	d := &durationpb.Duration{Seconds: seconds, Nanos: nanos}
	if err := d.CheckValid(); err != nil {
		return nil, fmt.Errorf("duration seconds=%d nanos=%d is not a valid google.protobuf.Duration: %w",
			seconds, nanos, err)
	}
	return d, nil
}

// FieldMaskPaths returns a FieldMask's paths, which every target stores as a
// list of strings.
//
// A nil mask yields nil rather than an empty slice, so a caller writing a
// FlatBuffers vector or a capnp list can tell "no mask" from "a mask selecting
// nothing" — a distinction AIP-134 leans on, since an absent update_mask means
// replace everything.
func FieldMaskPaths(m *fieldmaskpb.FieldMask) []string {
	if m == nil {
		return nil
	}
	return m.GetPaths()
}

// FieldMask rebuilds a FieldMask from the paths a target stored. Nil paths yield
// a nil mask, preserving the same distinction on the way back.
func FieldMask(paths []string) *fieldmaskpb.FieldMask {
	if paths == nil {
		return nil
	}
	return &fieldmaskpb.FieldMask{Paths: paths}
}
