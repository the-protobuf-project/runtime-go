// Package protobuf encodes stream payloads as protocol buffers.
//
// It is the codec to reach for when message volume matters: a proto body is
// several times smaller and faster to encode than the equivalent JSON, and it
// carries a schema that survives a rename.
//
//	import (
//	    "github.com/the-protobuf-project/runtime-go/streams/codec/protobuf"
//	    "github.com/the-protobuf-project/runtime-go/streams/redis"
//	)
//
//	s, err := redis.Connect(ctx, addr, redis.WithCodec(protobuf.Codec))
//
// What it demands in return is that every published value be a
// [google.golang.org/protobuf/proto.Message]. That is a real constraint and it
// is enforced rather than worked around: a plain struct handed to this codec is
// an error at the publish that did it, not a payload nobody can read.
package protobuf
