package buffers

import (
	"testing"

	capnp "capnproto.org/go/capnp/v3"
)

// writeCapnp builds a one-text-field message the way generated code would, and
// returns the marshaled bytes.
func writeCapnp(t *testing.T, text string, packed bool) []byte {
	t.Helper()

	w, err := Capnp()
	if err != nil {
		t.Fatalf("Capnp(): %v", err)
	}
	defer w.Release()

	root, rootErr := capnp.NewRootStruct(w.Segment(), capnp.ObjectSize{DataSize: 8, PointerCount: 1})
	if rootErr != nil {
		t.Fatalf("NewRootStruct: %v", rootErr)
	}
	if setErr := root.SetText(0, text); setErr != nil {
		t.Fatalf("SetText: %v", setErr)
	}
	root.SetUint64(0, 0xDEADBEEF)

	var data []byte
	if packed {
		data, err = w.Packed()
	} else {
		data, err = w.Bytes()
	}
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

// readCapnp reads the text and integer back out.
func readCapnp(t *testing.T, msg *capnp.Message) (string, uint64) {
	t.Helper()

	root, err := msg.Root()
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	s := root.Struct()
	p, err := s.Ptr(0)
	if err != nil {
		t.Fatalf("Ptr: %v", err)
	}
	return p.Text(), s.Uint64(0)
}

// TestBytesSurviveTheRelease pins an assumption this package makes about someone
// else's library.
//
// CapnpWriter.Bytes releases the arena immediately after marshaling, which is
// only safe because Message.Marshal builds a fresh buffer and copies every
// segment into it. That is capnp's behavior today, not a documented contract —
// so if it ever became a view into the segments, releasing would hand the caller
// memory the library had already taken back, and this is the test that says so.
func TestBytesSurviveTheRelease(t *testing.T) {
	data := writeCapnp(t, "robots/r1/sensors/s1", false)

	// Churn the arena pool, which is what would overwrite a buffer the caller
	// was still holding if Bytes had returned a view into it.
	for i := 0; i < 64; i++ {
		writeCapnp(t, "a different message entirely, of a different length", false)
	}

	msg, err := ReadCapnp(data)
	if err != nil {
		t.Fatalf("ReadCapnp: %v", err)
	}
	text, n := readCapnp(t, msg)
	if text != "robots/r1/sensors/s1" {
		t.Errorf("text = %q, want the original", text)
	}
	if n != 0xDEADBEEF {
		t.Errorf("uint64 = %#x, want 0xDEADBEEF", n)
	}
}

// TestPackedRoundTrips checks the packed encoding goes out and comes back, and
// that it is actually smaller for the zero-heavy layout it exists for.
func TestPackedRoundTrips(t *testing.T) {
	plain := writeCapnp(t, "sensor", false)
	packed := writeCapnp(t, "sensor", true)

	msg, err := ReadCapnpPacked(packed)
	if err != nil {
		t.Fatalf("ReadCapnpPacked: %v", err)
	}
	if text, _ := readCapnp(t, msg); text != "sensor" {
		t.Errorf("text = %q, want sensor", text)
	}
	if len(packed) >= len(plain) {
		t.Errorf("packed (%d bytes) is not smaller than plain (%d)", len(packed), len(plain))
	}
}

// TestWriterIsSpentAfterBytes checks a finished writer reports rather than
// marshaling an arena it has already handed back.
func TestWriterIsSpentAfterBytes(t *testing.T) {
	w, err := Capnp()
	if err != nil {
		t.Fatalf("Capnp(): %v", err)
	}
	if _, err := capnp.NewRootStruct(w.Segment(), capnp.ObjectSize{DataSize: 8}); err != nil {
		t.Fatalf("NewRootStruct: %v", err)
	}
	if _, err := w.Bytes(); err != nil {
		t.Fatalf("Bytes: %v", err)
	}

	if _, err := w.Bytes(); err == nil {
		t.Error("a second Bytes on a spent writer returned no error")
	}
	if _, err := w.Packed(); err == nil {
		t.Error("Packed on a spent writer returned no error")
	}
	w.Release() // the deferred one; must not panic
}
