package core

import (
	"encoding/binary"
	"fmt"

	"github.com/the-protobuf-project/runtime-go/streams"
)

// The frame every provider writes:
//
//	0xB7 0x01                marker, then version
//	uvarint len + codec name
//	uvarint len + message id
//	payload
//
// The id travels beside the payload rather than in a provider's own metadata,
// so a subscriber reports the id the publisher was given no matter which
// backend carried it. NATS could put it in a header and Redis pub/sub has
// nowhere to put it at all; agreeing on one frame is what keeps a message
// published through one provider readable through another.
//
// It is binary rather than JSON because the payload no longer has to be JSON:
// a protobuf body inside a JSON envelope would have to be base64, which is a
// third more bytes and an extra encode on every message.
const (
	frameMarker  = 0xB7
	frameVersion = 0x01
)

// maxField bounds a length read off the wire before anything is allocated for
// it, so a corrupt or hostile frame cannot ask for gigabytes.
const maxField = 1 << 20

// Pack encodes value with codec into a frame carrying id.
func Pack(codec streams.Codec, id string, value any) ([]byte, error) {
	if codec == nil {
		codec = streams.JSON
	}

	data, err := codec.Marshal(value)
	if err != nil {
		return nil, err
	}

	name := codec.Name()
	out := make([]byte, 0, 2+binary.MaxVarintLen64*2+len(name)+len(id)+len(data))
	out = append(out, frameMarker, frameVersion)
	out = binary.AppendUvarint(out, uint64(len(name)))
	out = append(out, name...)
	out = binary.AppendUvarint(out, uint64(len(id)))
	out = append(out, id...)
	out = append(out, data...)
	return out, nil
}

// Unpack turns a wire payload back into a message delivered on subject.
//
// The returned message carries the codec it was encoded with, so
// [streams.Message.Decode] reads it the way the publisher wrote it rather than
// the way this program is configured.
//
// A payload that is not one of our frames is an error rather than a message
// with an empty id: the id is what a caller correlates a delivery against, and
// inventing one would hide the fact that something else is writing to this
// subject.
func Unpack(reg *streams.Registry, subject string, payload []byte) (streams.Message, error) {
	if len(payload) < 2 || payload[0] != frameMarker {
		return streams.Message{}, fmt.Errorf("streams: message on %q was not published through this contract", subject)
	}
	if payload[1] != frameVersion {
		return streams.Message{}, fmt.Errorf("streams: message on %q uses frame version %d, and this program understands %d", subject, payload[1], frameVersion)
	}
	rest := payload[2:]

	name, rest, err := field(rest, subject, "codec name")
	if err != nil {
		return streams.Message{}, err
	}
	id, rest, err := field(rest, subject, "message id")
	if err != nil {
		return streams.Message{}, err
	}
	if len(id) == 0 {
		return streams.Message{}, fmt.Errorf("streams: message on %q carries no id", subject)
	}

	if reg == nil {
		reg = streams.NewRegistry()
	}
	codec, err := reg.Lookup(string(name))
	if err != nil {
		return streams.Message{}, err
	}

	return streams.NewMessage(string(id), subject, rest, codec), nil
}

// field reads one length-prefixed field, returning it and what follows.
func field(b []byte, subject, what string) (value, rest []byte, err error) {
	n, read := binary.Uvarint(b)
	if read <= 0 {
		return nil, nil, fmt.Errorf("streams: message on %q has a truncated %s", subject, what)
	}
	if n > maxField {
		return nil, nil, fmt.Errorf("streams: message on %q declares a %s of %d bytes, beyond the %d limit", subject, what, n, maxField)
	}
	b = b[read:]
	if uint64(len(b)) < n {
		return nil, nil, fmt.Errorf("streams: message on %q has a truncated %s", subject, what)
	}
	return b[:n], b[n:], nil
}
