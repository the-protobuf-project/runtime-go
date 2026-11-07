package opcua

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/streams/core"
)

// encode turns a published value into the variant written to a node.
//
// A value OPC UA has a type for is written as itself, so a PLC reading the node
// sees a Double rather than a frame. Anything else goes through the codec into
// a ByteString carrying this module's frame, which only another program using
// this contract will read — and only a ByteString node will accept. The id
// travels only in the frame.
func encode(codec streams.Codec, id string, value any) (*ua.Variant, error) {
	switch value.(type) {
	case int, uint, []int, []uint:
		return nil, fmt.Errorf("%w: OPC UA has no type for Go's int; "+
			"pass the width the node holds, such as int32 for an Int32 node", streams.ErrUnsupported)
	}

	if v, err := ua.NewVariant(value); err == nil {
		return v, nil
	}

	body, err := core.Pack(codec, id, value)
	if err != nil {
		return nil, err
	}
	v, err := ua.NewVariant(body)
	if err != nil {
		return nil, fmt.Errorf("opcua: cannot wrap an encoded value: %w", err)
	}
	return v, nil
}

// decode turns a node's value into a delivered message.
//
// A ByteString holding this module's frame is unpacked, so it decodes the way
// its publisher wrote it. Any other value — written by a PLC, an HMI, or this
// package publishing a native type — is carried as JSON, which reads back into
// the matching Go type: a Double into a float64, a DateTime into a time.Time.
//
// A value whose status is not good is an error rather than a message: a node
// reporting that its sensor is disconnected has no value to hand anyone, and
// delivering a zero would look like a reading.
func decode(reg *streams.Registry, subject string, dv *ua.DataValue) (streams.Message, error) {
	if dv == nil {
		return streams.Message{}, fmt.Errorf("opcua: %s reported no value", subject)
	}
	// The top two bits are the severity, and only good (00) is a reading.
	// Uncertain is refused along with bad: a message has nowhere to say that
	// its value may be wrong.
	if uint32(dv.Status)>>30 != 0 {
		return streams.Message{}, fmt.Errorf("opcua: %s reported status: %w", subject, dv.Status)
	}
	if dv.Value == nil || dv.Value.Value() == nil {
		return streams.Message{}, fmt.Errorf("opcua: %s holds no value", subject)
	}

	raw := dv.Value.Value()
	if b, ok := raw.([]byte); ok {
		if msg, err := core.Unpack(reg, subject, b); err == nil {
			return msg, nil
		}
		// Not a frame: a ByteString written by something else, delivered as
		// the bytes it is.
	}

	if nonFinite(raw) {
		// JSON has no NaN or infinity, and a sensor reporting one is still
		// reporting.
		data, err := floats.Marshal(raw)
		if err != nil {
			return streams.Message{}, fmt.Errorf("opcua: cannot carry the %T value of %s: %w", raw, subject, err)
		}
		return streams.NewMessage(sampleID(dv), subject, data, floats), nil
	}

	data, err := json.Marshal(raw)
	if err != nil {
		return streams.Message{}, fmt.Errorf("opcua: cannot carry the %T value of %s: %w", raw, subject, err)
	}
	return streams.NewMessage(sampleID(dv), subject, data, streams.JSON), nil
}

// sampleID is the id of a native value: its source timestamp, which is what
// identifies a sample in OPC UA, or the server's when the source gave none.
func sampleID(dv *ua.DataValue) string {
	t := dv.SourceTimestamp
	if t.IsZero() {
		t = dv.ServerTimestamp
	}
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
