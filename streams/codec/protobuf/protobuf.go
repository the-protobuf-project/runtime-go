package protobuf

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/streams"
)

// Codec encodes and decodes [proto.Message] values.
var Codec streams.Codec = codec{}

type codec struct{}

// Name is what travels in the frame, so it must not change once anything has
// been published under it.
func (codec) Name() string { return "protobuf" }

func (codec) Marshal(v any) ([]byte, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("streams: the protobuf codec needs a proto.Message, and %T is not one", v)
	}
	b, err := proto.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("streams: cannot encode value as protobuf: %w", err)
	}
	return b, nil
}

func (codec) Unmarshal(data []byte, dest any) error {
	if dest == nil {
		return fmt.Errorf("streams: decode destination is nil")
	}
	m, ok := dest.(proto.Message)
	if !ok {
		return fmt.Errorf("streams: the protobuf codec decodes into a proto.Message, and %T is not one", dest)
	}
	if err := proto.Unmarshal(data, m); err != nil {
		return fmt.Errorf("streams: cannot decode message as protobuf: %w", err)
	}
	return nil
}
