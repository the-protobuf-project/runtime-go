package streams

import (
	"encoding/json"
	"fmt"
)

// Codec turns a caller's model into bytes and back.
//
// It is the seam between what a program wants to send and what a broker
// carries. The default is [JSON], which needs nothing of a model; a program
// that would rather pay less per message can hand a provider a codec that
// demands more of one — see runtime-go/streams/codec/protobuf.
//
// The name travels with every message, so a subscriber decodes a payload the
// way it was encoded rather than the way it happens to be configured. Two
// programs disagreeing about the codec is then an error naming both, instead of
// a struct that silently decodes to zero values.
type Codec interface {
	// Name identifies the codec on the wire. It is compared literally, so it
	// has to stay stable once anything has been published under it.
	Name() string

	// Marshal encodes a value.
	Marshal(v any) ([]byte, error)

	// Unmarshal decodes into dest, which is a non-nil pointer.
	Unmarshal(data []byte, dest any) error
}

// JSON is the default codec: it encodes any model without being told anything
// about it, which is what makes it the right default and the wrong choice at
// volume.
var JSON Codec = jsonCodec{}

type jsonCodec struct{}

func (jsonCodec) Name() string { return "json" }

func (jsonCodec) Marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("streams: cannot encode value as JSON: %w", err)
	}
	return b, nil
}

func (jsonCodec) Unmarshal(data []byte, dest any) error {
	if dest == nil {
		return fmt.Errorf("streams: decode destination is nil")
	}
	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("streams: cannot decode message as JSON: %w", err)
	}
	return nil
}

// Registry resolves a codec name read off the wire back to a codec.
//
// A provider decodes with whatever encoded the message, so it needs to find a
// codec by name and not merely hold the one it was configured with. Registering
// is additive and safe to do from an init function; JSON is always present.
type Registry struct {
	byName map[string]Codec
}

// NewRegistry returns a registry holding the given codecs and [JSON].
func NewRegistry(codecs ...Codec) *Registry {
	r := &Registry{byName: map[string]Codec{JSON.Name(): JSON}}
	for _, c := range codecs {
		if c != nil {
			r.byName[c.Name()] = c
		}
	}
	return r
}

// Lookup returns the codec registered under name.
//
// An unknown name is an error rather than a fallback to JSON: a payload encoded
// by something this program does not have is not a payload it can read, and
// guessing would hand the caller a zero value that looks like data.
func (r *Registry) Lookup(name string) (Codec, error) {
	if c, ok := r.byName[name]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("%w: this message was encoded with the %q codec, which this program does not have; register it with streams.NewRegistry", ErrUnsupported, name)
}

// Encoded is a value its publisher already encoded, published as it is.
//
// A provider frames Data under Codec instead of encoding the value with its
// own codec, so a caller that owns its encoding (a typed topic whose wire
// format is FlatBuffers or Cap'n Proto, converted by code buffers generates)
// still publishes through any provider, and its subscribers read the codec it
// used off the frame.
type Encoded struct {
	// Codec names the encoding, as [Codec.Name] would.
	Codec string

	// Data is the encoded payload.
	Data []byte
}

// deferredCodec stands in for a codec this program does not have.
//
// A message encoded with one is still delivered, so a caller that decodes the
// payload itself, a typed topic holding the codec, can read it; decoding it
// through [Message.Decode] fails, naming the codec, exactly as looking it up
// would have.
type deferredCodec struct {
	name string
	err  error
}

func (d deferredCodec) Name() string                { return d.name }
func (d deferredCodec) Marshal(any) ([]byte, error) { return nil, d.err }
func (d deferredCodec) Unmarshal([]byte, any) error { return d.err }

// Deferred returns a codec for a message whose codec, name, this program does
// not have: [Message.Decode] fails with err, and [Message.CodecName] still
// reports name. Providers reach it through runtime-go/streams/core.
func Deferred(name string, err error) Codec {
	return deferredCodec{name: name, err: err}
}
