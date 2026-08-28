package buffers

// capnp.go owns the Cap'n Proto message lifecycle, which generated code would
// otherwise repeat once per message and get subtly wrong once per message.
//
// A capnp message is not a value, it is an arena: it holds segment buffers that
// the library will reuse once released, and everything read out of it points
// into those buffers. Whether releasing is safe therefore depends on whether the
// bytes you kept were copied out or aliased — and the answer is different for
// the two directions, which is exactly why it belongs here rather than in a
// generated file that has to be right N times.
//
// Writing is safe to release: [Message.Marshal] builds a fresh buffer and copies
// every segment into it, so the bytes it returns outlive the arena.
//
// Reading is not: [Unmarshal] wraps the caller's slice without copying, so the
// decoded message — and every string and struct reached through it — is only
// valid while that slice is unmodified. A conversion reads such a message into a
// protobuf one, which does copy, so the borrow ends when the conversion returns.

import (
	"errors"

	capnp "capnproto.org/go/capnp/v3"
)

// CapnpWriter is a Cap'n Proto message being built.
//
// It exists so a conversion cannot forget to release the arena, and cannot
// release it at the wrong moment: [CapnpWriter.Bytes] marshals and releases in
// one step, and [CapnpWriter.Release] is safe to defer alongside it.
type CapnpWriter struct {
	// msg is the message being built, or nil once released.
	msg *capnp.Message
	// seg is its first and only segment.
	seg *capnp.Segment
}

// Capnp opens a single-segment message for a conversion to write into.
//
// Single-segment because a converted protobuf message is a bounded tree with no
// reason to span segments, and a single segment marshals without a segment table
// — the smaller and more portable of the two encodings.
func Capnp() (*CapnpWriter, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, err
	}
	return &CapnpWriter{msg: msg, seg: seg}, nil
}

// Segment returns the segment to allocate the root struct in.
func (w *CapnpWriter) Segment() *capnp.Segment { return w.seg }

// Message returns the underlying message, for the rare caller that needs it.
func (w *CapnpWriter) Message() *capnp.Message { return w.msg }

// Bytes marshals the message and releases the arena.
//
// Releasing here is safe and deliberate: Marshal copies every segment into a new
// buffer, so what is returned does not point into the arena being recycled. The
// writer is spent afterwards, and a second call reports that rather than
// marshaling a released message.
func (w *CapnpWriter) Bytes() ([]byte, error) {
	if w.msg == nil {
		return nil, errors.New("capnp writer: already finished")
	}
	data, err := w.msg.Marshal()
	w.Release()
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Packed is Bytes in Cap'n Proto's packed encoding, which compresses the zero
// bytes a fixed-layout format is full of.
func (w *CapnpWriter) Packed() ([]byte, error) {
	if w.msg == nil {
		return nil, errors.New("capnp writer: already finished")
	}
	data, err := w.msg.MarshalPacked()
	w.Release()
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Release returns the arena's buffers for reuse. It is idempotent, so a
// conversion can defer it and still call Bytes on the success path.
func (w *CapnpWriter) Release() {
	if w.msg == nil {
		return
	}
	w.msg.Release()
	w.msg, w.seg = nil, nil
}

// ReadCapnp decodes a Cap'n Proto message for a conversion to read out of.
//
// The returned message *borrows* data rather than copying it, so it is valid
// only while data is unmodified. That suits the one caller this is for — a
// conversion reads it straight into a protobuf message, which copies — and would
// not suit a caller that kept the result.
func ReadCapnp(data []byte) (*capnp.Message, error) { return capnp.Unmarshal(data) }

// ReadCapnpPacked decodes the packed encoding, with the same borrowing rule.
func ReadCapnpPacked(data []byte) (*capnp.Message, error) { return capnp.UnmarshalPacked(data) }
