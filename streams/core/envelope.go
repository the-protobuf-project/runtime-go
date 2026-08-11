package core

import (
	"encoding/json"
	"fmt"

	"github.com/the-protobuf-project/runtime-go/streams"
)

// Envelope is the wire form every provider writes.
//
// The id travels beside the payload rather than in a provider's own metadata,
// so a subscriber reports the id the publisher was given no matter which
// backend carried it. NATS could put it in a header and Redis pub/sub has
// nowhere to put it at all; agreeing on one frame is what keeps a message
// published through one provider readable through another.
type Envelope struct {
	ID   string          `json:"id"`
	Data json.RawMessage `json:"data"`
}

// Pack encodes value into an envelope carrying id.
func Pack(id string, value any) ([]byte, error) {
	data, err := streams.Encode(value)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(Envelope{ID: id, Data: data})
	if err != nil {
		return nil, fmt.Errorf("streams: cannot encode message: %w", err)
	}
	return body, nil
}

// Unpack turns a wire payload back into a message delivered on subject.
//
// A payload that is not an envelope is an error rather than a message with an
// empty id: the id is what a caller correlates a delivery against, and inventing
// one would hide the fact that something else is writing to this subject.
func Unpack(subject string, payload []byte) (streams.Message, error) {
	var e Envelope
	if err := json.Unmarshal(payload, &e); err != nil {
		return streams.Message{}, fmt.Errorf("streams: malformed message on %q: %w", subject, err)
	}
	// Well-formed JSON that is not an envelope decodes into an empty one,
	// because unknown fields are ignored. A provider always packs an id, so an
	// absent one means this payload came from somewhere else — reporting that
	// beats handing over a message whose id silently became "".
	if e.ID == "" {
		return streams.Message{}, fmt.Errorf("streams: message on %q carries no id and was not published through this contract", subject)
	}
	return streams.Message{ID: e.ID, Subject: subject, Data: e.Data}, nil
}
