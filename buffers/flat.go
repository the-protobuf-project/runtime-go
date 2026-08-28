package buffers

// flat.go pools FlatBuffers builders, and copies on the way out.
//
// The copy is the whole point, and skipping it is the bug this file exists to
// make impossible. flatbuffers.Builder.FinishedBytes returns a slice *into* the
// builder's own array:
//
//	func (b *Builder) FinishedBytes() []byte { return b.Bytes[b.Head():] }
//
// and Reset keeps that array to refill it. So a pooled builder hands the next
// conversion the same memory the last caller is still holding, and the first
// message silently becomes the second one — at a distance, long after the call
// that caused it, in a program that looks correct.
//
// Pooling is still worth it: a builder is an allocation plus a growing buffer,
// and a service converting on a hot path allocates one per message otherwise.
// The trade is one copy of the finished message, which is a copy the caller
// almost always needed anyway, in exchange for never reallocating the scratch
// space that produced it.

import (
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
)

// flatInitialSize is the capacity a fresh builder starts with. It is a starting
// point rather than a limit — the builder grows — chosen to cover an ordinary
// message without a reallocation.
const flatInitialSize = 1024

// flatPool holds idle builders. The zero Builder is not usable, so New is set
// rather than relying on the pool's nil.
var flatPool = sync.Pool{
	New: func() any { return flatbuffers.NewBuilder(flatInitialSize) },
}

// FlatBuilder is a pooled FlatBuffers builder for one conversion.
type FlatBuilder struct {
	// b is the pooled builder, or nil once returned.
	b *flatbuffers.Builder
}

// Flat takes a builder from the pool.
//
// Pair it with a deferred [FlatBuilder.Release]; [FlatBuilder.Finish] releases
// on the success path and Release is idempotent, so the two compose without the
// caller tracking which one ran.
func Flat() *FlatBuilder {
	b, _ := flatPool.Get().(*flatbuffers.Builder)
	b.Reset()
	return &FlatBuilder{b: b}
}

// Builder returns the builder to write the message into.
//
// It is valid until Finish or Release, and must not be retained past either —
// the builder goes back to the pool and its buffer is refilled.
func (f *FlatBuilder) Builder() *flatbuffers.Builder { return f.b }

// Finish completes the message at root and returns it as bytes the caller owns.
//
// The bytes are copied out of the builder before it is returned to the pool. See
// this file's comment for why that is not an optimization to remove.
func (f *FlatBuilder) Finish(root flatbuffers.UOffsetT) []byte {
	if f.b == nil {
		return nil
	}
	f.b.Finish(root)

	view := f.b.FinishedBytes()
	out := make([]byte, len(view))
	copy(out, view)

	f.Release()
	return out
}

// FinishSizePrefixed is Finish with FlatBuffers' four-byte length prefix, which
// is what a reader needs when messages are framed on a stream rather than each
// held in its own slice.
func (f *FlatBuilder) FinishSizePrefixed(root flatbuffers.UOffsetT) []byte {
	if f.b == nil {
		return nil
	}
	f.b.FinishSizePrefixed(root)

	view := f.b.FinishedBytes()
	out := make([]byte, len(view))
	copy(out, view)

	f.Release()
	return out
}

// Release returns the builder to the pool without finishing, for the error path.
// It is idempotent.
func (f *FlatBuilder) Release() {
	if f.b == nil {
		return
	}
	flatPool.Put(f.b)
	f.b = nil
}
