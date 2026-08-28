package buffers

import (
	"bytes"
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
)

// buildString writes a one-field message holding s and returns the finished
// bytes, going through the pool exactly as generated code does.
func buildString(s string) []byte {
	f := Flat()
	defer f.Release()

	b := f.Builder()
	off := b.CreateString(s)
	b.StartObject(1)
	b.PrependUOffsetTSlot(0, off, 0)
	return f.Finish(b.EndObject())
}

// TestFinishedBytesSurvivePoolReuse is the reason this pool copies.
//
// flatbuffers.Builder.FinishedBytes returns a slice into the builder's own
// array, and Reset keeps that array to refill it — so a pool that handed the
// slice straight out would let the next conversion overwrite the last one's
// result. The failure is silent, arrives long after the call that caused it, and
// looks like data corruption rather than a lifetime bug.
//
// Returning the builder to the pool many times over is what makes this
// deterministic: if the bytes aliased the builder, one of the reuses would
// certainly land on them.
func TestFinishedBytesSurvivePoolReuse(t *testing.T) {
	first := buildString("the first message")

	keep := make([]byte, len(first))
	copy(keep, first)

	for i := 0; i < 64; i++ {
		buildString("a completely different message that refills the same buffer")
	}

	if !bytes.Equal(first, keep) {
		t.Errorf("the first message changed after the builder was reused:\n got %q\nwant %q", first, keep)
	}
}

// TestTwoLiveResultsAreIndependent covers the same hazard from the other side:
// two results held at once must not be the same memory.
func TestTwoLiveResultsAreIndependent(t *testing.T) {
	a := buildString("aaaaaaaaaaaaaaaa")
	b := buildString("bbbbbbbbbbbbbbbb")

	if bytes.Equal(a, b) {
		t.Fatal("two different messages produced identical bytes; the pool is aliasing")
	}
	if len(a) > 0 && len(b) > 0 && &a[0] == &b[0] {
		t.Error("two results share a backing array")
	}
}

// TestReleaseIsIdempotent lets a conversion defer Release and still call Finish
// on the success path, which is the shape generated code uses.
func TestReleaseIsIdempotent(t *testing.T) {
	f := Flat()
	b := f.Builder()
	b.StartObject(0)
	_ = f.Finish(b.EndObject())

	f.Release() // the deferred one, after Finish already released
	f.Release() // and again, for good measure
}

// TestFinishAfterReleaseIsNotAUseAfterFree checks the spent builder reports
// rather than writing into a builder another conversion now owns.
func TestFinishAfterReleaseIsNotAUseAfterFree(t *testing.T) {
	f := Flat()
	f.Release()

	if got := f.Finish(flatbuffers.UOffsetT(0)); got != nil {
		t.Errorf("Finish on a released builder returned %v, want nil", got)
	}
	if got := f.FinishSizePrefixed(flatbuffers.UOffsetT(0)); got != nil {
		t.Errorf("FinishSizePrefixed on a released builder returned %v, want nil", got)
	}
}

// TestSizePrefixedIsLongerByFourBytes checks the framing variant actually frames.
func TestSizePrefixedIsLongerByFourBytes(t *testing.T) {
	plain := buildString("x")

	f := Flat()
	b := f.Builder()
	off := b.CreateString("x")
	b.StartObject(1)
	b.PrependUOffsetTSlot(0, off, 0)
	prefixed := f.FinishSizePrefixed(b.EndObject())

	if len(prefixed) != len(plain)+4 {
		t.Errorf("size-prefixed length = %d, want %d", len(prefixed), len(plain)+4)
	}
}
