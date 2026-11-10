package topic

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// Codec encodes one message type for the wire, and is a topic's own: the
// wire format can differ topic by topic while every topic stays typed to the
// message the proto defines.
//
// protobuf and JSON are here. Other formats (FlatBuffers, Cap'n Proto,
// Thrift) are generated: protoc-gen-buffers writes the conversion between a
// proto message and the format, and protoc-gen-streams wraps it in a Codec,
// so a Topic[*pb.Order] can carry FlatBuffers while its callers publish and
// receive *pb.Order.
//
// Name travels in every frame, so a subscriber decodes with the codec the
// publisher used. It is compared literally and must stay stable once anything
// has been published under it.
type Codec[T any] interface {
	// Name identifies the encoding on the wire.
	Name() string

	// Encode turns a message into bytes.
	Encode(T) ([]byte, error)

	// Decode turns bytes back into a message.
	Decode([]byte) (T, error)
}

// JSON returns the codec encoding T as JSON, under the name "json", which a
// provider's own [streams.JSON] also writes, so either side can read the
// other.
func JSON[T any]() Codec[T] { return jsonCodec[T]{decode: decoder[T]()} }

type jsonCodec[T any] struct {
	decode func(into func(dest any) error) (T, error)
}

func (jsonCodec[T]) Name() string { return "json" }

func (jsonCodec[T]) Encode(v T) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("streams: cannot encode value as JSON: %w", err)
	}
	return b, nil
}

func (c jsonCodec[T]) Decode(data []byte) (T, error) {
	v, err := c.decode(func(dest any) error { return json.Unmarshal(data, dest) })
	if err != nil {
		return v, fmt.Errorf("streams: cannot decode message as JSON: %w", err)
	}
	return v, nil
}

// Protobuf returns the codec encoding a generated protobuf message, under the
// name "protobuf", which runtime-go/streams/codec/protobuf and runtime-rs's
// Protobuf also write.
func Protobuf[T proto.Message]() Codec[T] { return protobufCodec[T]{} }

type protobufCodec[T proto.Message] struct{}

func (protobufCodec[T]) Name() string { return "protobuf" }

func (protobufCodec[T]) Encode(v T) ([]byte, error) {
	b, err := proto.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("streams: cannot encode value as protobuf: %w", err)
	}
	return b, nil
}

func (protobufCodec[T]) Decode(data []byte) (T, error) {
	// A nil pointer to a generated message still knows its type, so it makes
	// the fresh message to decode into.
	var zero T
	v, ok := zero.ProtoReflect().New().Interface().(T)
	if !ok {
		return zero, fmt.Errorf("streams: cannot make a %T to decode into", zero)
	}
	if err := proto.Unmarshal(data, v); err != nil {
		return zero, fmt.Errorf("streams: cannot decode message as protobuf: %w", err)
	}
	return v, nil
}
