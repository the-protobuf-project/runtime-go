// Package buffers is the Go runtime for the converters protoc-gen-buffers
// generates: the half of a proto-to-FlatBuffers-or-Cap'n-Proto conversion that
// does not depend on which message is being converted.
//
// # Why a conversion has to be generated at all
//
// protobuf is not the only encoding a system ends up speaking, but it is usually
// the one the types are defined in. buffers turns those definitions into
// FlatBuffers, Cap'n Proto and Thrift schema; this package is what lets a running
// Go program move a value between them.
//
// It cannot do that by reflection, and the reason is worth stating because the
// alternative looks plausible. protobuf has protoreflect, so walking the source
// message is easy — but there is nothing to walk *into*. capnpc-go emits raw
// accessors over fixed offsets, `Ptr(0)` and `SetText(0, v)`, and flatc emits
// append-only positional builders. Neither has a dynamic, schema-driven API a
// generic walker could drive, so the field-by-field copy has to be code, and code
// that knows both sides is code that must be generated.
//
// What is left over is everything a copy needs that is *not* per-field: opening
// an arena, pooling a builder, converting a Timestamp, and saying which field
// failed. That is this package.
//
// # The shape of the generated API
//
// Conversions read as two-link chains named after the format, in both directions:
//
//	data, err := bridge.Wrap(sensor).FlatBuffers()   // proto  -> flatbuffers
//	data, err := bridge.Wrap(sensor).CapnProto()     // proto  -> cap'n proto
//
//	sensor, err := bridge.FlatBuffers(data).Sensor() // flatbuffers -> proto
//	sensor, err := bridge.CapnProto(data).Sensor()   // cap'n proto -> proto
//
// The format is the method name because the format is the thing a caller already
// knows. There is no To, no From and no Bytes: a verb prefix makes the call
// harder to recall without saying anything the type signature does not, and the
// same vocabulary is meant to read the same way in every language runtime, not
// only this one.
//
// Go forbids methods on a type from another package, so `Wrap` is not decoration
// — it is the only way to get receiver syntax over a message that belongs to
// protoc-gen-go. It costs one call and no allocation.
//
// # What this package guarantees
//
// Lifecycle and diagnosis, which are the two things a hand-written conversion
// gets wrong.
//
// A Cap'n Proto message owns segment buffers that are worth reusing and unsafe to
// reuse carelessly, so the arena is opened and released here rather than in
// generated code that would have to repeat the pattern per message. A FlatBuffers
// builder is expensive to allocate and cheap to reset, so it is pooled.
//
// And a conversion that fails in the fourth field of a nested message has to say
// so. Every error carries the path it happened at — `mount.orientation.w` — built
// up as the failure unwinds, because "cannot set text" on its own names nothing a
// reader can act on.
package buffers
