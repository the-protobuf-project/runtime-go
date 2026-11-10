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
// and, for a transport with no header mechanism of its own (Redis pub/sub and
// scheduled delivery, ZeroMQ) when there are headers to carry:
//
//	0xB7 0x02                marker, then version
//	uvarint len + codec name
//	uvarint len + message id
//	uvarint count, then count * (uvarint len + key, uvarint len + value)
//	payload
//
// A message without headers is always version 1, so a reader that knows only
// version 1 still reads everything published without headers.
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
	frameMarker         = 0xB7
	frameVersion        = 0x01
	frameVersionHeaders = 0x02
)

// maxHeaders bounds the header count read off the wire, so a corrupt count
// cannot make a reader loop for long.
const maxHeaders = 1024

// maxField bounds a length read off the wire before anything is allocated for
// it, so a corrupt or hostile frame cannot ask for gigabytes.
const maxField = 1 << 20

// Pack encodes value with codec into a frame carrying id.
func Pack(codec streams.Codec, id string, value any) ([]byte, error) {
	return PackWith(codec, id, value, nil)
}

// PackWith is [Pack] carrying headers in the frame, for a transport that has
// nowhere else to put them. Without headers it writes exactly what Pack does.
//
// A [streams.Encoded] value is framed as it is, under its own codec name,
// rather than encoded with codec.
func PackWith(codec streams.Codec, id string, value any, headers streams.Headers) ([]byte, error) {
	if codec == nil {
		codec = streams.JSON
	}

	var (
		data []byte
		name string
	)
	if enc, ok := value.(streams.Encoded); ok {
		if enc.Codec == "" {
			return nil, fmt.Errorf("streams: an Encoded value has to name its codec")
		}
		data, name = enc.Data, enc.Codec
	} else {
		var err error
		if data, err = codec.Marshal(value); err != nil {
			return nil, err
		}
		name = codec.Name()
	}
	if len(headers) == 0 {
		out := make([]byte, 0, 2+binary.MaxVarintLen64*2+len(name)+len(id)+len(data))
		out = append(out, frameMarker, frameVersion)
		out = binary.AppendUvarint(out, uint64(len(name)))
		out = append(out, name...)
		out = binary.AppendUvarint(out, uint64(len(id)))
		out = append(out, id...)
		out = append(out, data...)
		return out, nil
	}

	size := 2 + binary.MaxVarintLen64*3 + len(name) + len(id) + len(data)
	for k, v := range headers {
		size += binary.MaxVarintLen64*2 + len(k) + len(v)
	}
	out := make([]byte, 0, size)
	out = append(out, frameMarker, frameVersionHeaders)
	out = binary.AppendUvarint(out, uint64(len(name)))
	out = append(out, name...)
	out = binary.AppendUvarint(out, uint64(len(id)))
	out = append(out, id...)
	out = binary.AppendUvarint(out, uint64(len(headers)))
	for _, k := range headers.Keys() {
		out = binary.AppendUvarint(out, uint64(len(k)))
		out = append(out, k...)
		v := headers[k]
		out = binary.AppendUvarint(out, uint64(len(v)))
		out = append(out, v...)
	}
	out = append(out, data...)
	return out, nil
}

// Unpack turns a wire payload back into a message delivered on subject.
//
// The returned message carries the codec it was encoded with, so
// [streams.Message.Decode] reads it the way the publisher wrote it rather than
// the way this program is configured. A codec the registry does not have is
// not an error here: the message is delivered, its Decode fails naming the
// codec, and a caller holding that codec reads [streams.Message.Data] itself.
//
// A payload that is not one of our frames is an error rather than a message
// with an empty id: the id is what a caller correlates a delivery against, and
// inventing one would hide the fact that something else is writing to this
// subject.
func Unpack(reg *streams.Registry, subject string, payload []byte) (streams.Message, error) {
	if len(payload) < 2 || payload[0] != frameMarker {
		return streams.Message{}, fmt.Errorf("streams: message on %q was not published through this contract", subject)
	}
	version := payload[1]
	if version != frameVersion && version != frameVersionHeaders {
		return streams.Message{}, fmt.Errorf("streams: message on %q uses frame version %d, and this program understands %d and %d", subject, version, frameVersion, frameVersionHeaders)
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

	var headers streams.Headers
	if version == frameVersionHeaders {
		count, read := binary.Uvarint(rest)
		if read <= 0 {
			return streams.Message{}, fmt.Errorf("streams: message on %q has a truncated header count", subject)
		}
		if count > maxHeaders {
			return streams.Message{}, fmt.Errorf("streams: message on %q declares %d headers, beyond the %d limit", subject, count, maxHeaders)
		}
		rest = rest[read:]
		headers = make(streams.Headers, count)
		for range count {
			var key, value []byte
			if key, rest, err = field(rest, subject, "header key"); err != nil {
				return streams.Message{}, err
			}
			if value, rest, err = field(rest, subject, "header value"); err != nil {
				return streams.Message{}, err
			}
			headers[string(key)] = string(value)
		}
	}

	if reg == nil {
		reg = streams.NewRegistry()
	}
	codec, err := reg.Lookup(string(name))
	if err != nil {
		// Delivered rather than dropped: a typed topic holding this codec
		// decodes the payload itself, and Message.Decode still fails naming it.
		codec = streams.Deferred(string(name), err)
	}

	msg := streams.NewMessage(string(id), subject, rest, codec)
	msg.Headers = headers
	return msg, nil
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
