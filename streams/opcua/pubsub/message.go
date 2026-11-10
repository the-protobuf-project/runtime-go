package pubsub

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gopcua/opcua/ua"
)

// OPC UA PubSub JSON messages (Part 14, 7.2.5): the NetworkMessage, its
// DataSetMessages, and the DataSetMetaData announcement.
//
// The envelope is written field by field, absent fields omitted as Part 14
// asks. Reading is more forgiving than writing, because other publishers
// choose other forms: a NetworkMessage may arrive without its header, as one
// DataSetMessage or an array of them, a PublisherId may be a number, and
// OPC Publisher names the WriterGroup DataSetWriterGroup.

const (
	// MessageTypeData is the MessageType of a NetworkMessage carrying data.
	MessageTypeData = "ua-data"
	// MessageTypeMetadata is the MessageType of a DataSetMetaData announcement.
	MessageTypeMetadata = "ua-metadata"
	// MessageTypeKeepalive is the DataSetMessage type of a keep-alive, which
	// carries no Payload.
	MessageTypeKeepalive = "ua-keepalive"
)

// Field is one named value of a DataSet.
type Field struct {
	// Name is the field's name, its key in a DataSetMessage's Payload.
	Name string
	// Value is the field's value, status and timestamps.
	Value *ua.DataValue
}

// NetworkMessage is a Part 14 JSON NetworkMessage with MessageType ua-data.
type NetworkMessage struct {
	// MessageID identifies the message; it is the id a publish returns.
	MessageID string
	// PublisherID is the publisher that sent it.
	PublisherID string
	// WriterGroupName is the WriterGroup it was sent for.
	WriterGroupName string
	// DataSetClassID is the DataSetClass every DataSetMessage in it belongs
	// to, if one.
	DataSetClassID string
	// Messages are the DataSetMessages, in the order they were written.
	Messages []DataSetMessage
}

// DataSetMessage is one DataSetWriter's values at one moment.
//
// The pointer fields are optional in Part 14, and nil leaves them out; an
// empty string, a zero DataSetWriterID (which Part 14 reserves) and a zero
// Timestamp do the same.
type DataSetMessage struct {
	// DataSetWriterID is the writer's numeric id within its publisher.
	DataSetWriterID uint16
	// DataSetWriterName is the writer's name.
	DataSetWriterName string
	// PublisherID is the publisher, when the NetworkMessage header does not
	// carry it.
	PublisherID string
	// WriterGroupName is the WriterGroup, when the NetworkMessage header does
	// not carry it.
	WriterGroupName string
	// SequenceNumber counts the writer's messages, so a reader can tell when
	// it missed one.
	SequenceNumber *uint32
	// MetaDataVersion is the version of the DataSetMetaData the Payload
	// follows.
	MetaDataVersion *ua.ConfigurationVersionDataType
	// Timestamp is when the message was written.
	Timestamp time.Time
	// Status is the status of the DataSet as a whole.
	Status *ua.StatusCode
	// MessageType is ua-keyframe, ua-deltaframe, ua-event or ua-keepalive;
	// empty means a key frame.
	MessageType string
	// Payload is the DataSet's fields, by name, in order.
	Payload []Field
}

// MetaDataMessage is a Part 14 JSON DataSetMetaData message (MessageType
// ua-metadata): the names, types and version of a writer's fields.
type MetaDataMessage struct {
	// MessageID identifies the message.
	MessageID string
	// PublisherID is the publisher the metadata describes.
	PublisherID string
	// DataSetWriterID is the writer's numeric id.
	DataSetWriterID uint16
	// WriterGroupName is the writer's WriterGroup.
	WriterGroupName string
	// DataSetWriterName is the writer's name.
	DataSetWriterName string
	// Timestamp is when the metadata was sent.
	Timestamp time.Time
	// MetaData is the metadata itself.
	MetaData ua.DataSetMetaDataType
}

// codecError is how every encode and decode failure in this package reads:
// a malformed message is not something a retry fixes.
func codecError(what string, err error) error {
	return fmt.Errorf("opcua pubsub: cannot %s: %w", what, err)
}

// putStr writes name: value only when value is set; Part 14 omits absent
// fields rather than nulling them.
func putStr(w *jsonWriter, name, value string) {
	if value != "" {
		w.name(name)
		w.str(value)
	}
}

// putVersion writes a ConfigurationVersion, each half only when it is not
// zero, as async-opcua writes the structure.
func putVersion(w *jsonWriter, v *ua.ConfigurationVersionDataType) {
	w.beginObject()
	if v != nil {
		if v.MajorVersion != 0 {
			w.name("MajorVersion")
			w.uint(uint64(v.MajorVersion))
		}
		if v.MinorVersion != 0 {
			w.name("MinorVersion")
			w.uint(uint64(v.MinorVersion))
		}
	}
	w.endObject()
}

// JSON encodes the message as Part 14 JSON. MessageType is always ua-data.
func (n *NetworkMessage) JSON() ([]byte, error) {
	w := &jsonWriter{buf: make([]byte, 0, 256)}
	w.beginObject()
	putStr(w, "MessageId", n.MessageID)
	w.name("MessageType")
	w.str(MessageTypeData)
	putStr(w, "PublisherId", n.PublisherID)
	putStr(w, "WriterGroupName", n.WriterGroupName)
	putStr(w, "DataSetClassId", n.DataSetClassID)
	w.name("Messages")
	w.beginArray()
	for i := range n.Messages {
		if err := n.Messages[i].write(w); err != nil {
			return nil, codecError("encode a JSON message", err)
		}
	}
	w.endArray()
	w.endObject()
	return w.buf, nil
}

// PublisherOf is the publisher of one of its DataSetMessages: the message's
// own, or the header's.
func (n *NetworkMessage) PublisherOf(m *DataSetMessage) string {
	if m.PublisherID != "" {
		return m.PublisherID
	}
	return n.PublisherID
}

// write writes one DataSetMessage object, only the fields that are set.
func (m *DataSetMessage) write(w *jsonWriter) error {
	w.beginObject()
	if m.DataSetWriterID != 0 {
		w.name("DataSetWriterId")
		w.uint(uint64(m.DataSetWriterID))
	}
	putStr(w, "DataSetWriterName", m.DataSetWriterName)
	putStr(w, "PublisherId", m.PublisherID)
	putStr(w, "WriterGroupName", m.WriterGroupName)
	if m.SequenceNumber != nil {
		w.name("SequenceNumber")
		w.uint(uint64(*m.SequenceNumber))
	}
	if m.MetaDataVersion != nil {
		w.name("MetaDataVersion")
		putVersion(w, m.MetaDataVersion)
	}
	if !m.Timestamp.IsZero() {
		w.name("Timestamp")
		w.str(formatTicks(m.Timestamp))
	}
	if m.Status != nil {
		w.name("Status")
		w.uint(uint64(*m.Status))
	}
	putStr(w, "MessageType", m.MessageType)
	w.name("Payload")
	w.beginObject()
	for _, f := range m.Payload {
		w.name(f.Name)
		if err := putDataValue(w, f.Value, formatTicks); err != nil {
			return fmt.Errorf("field %q: %w", f.Name, err)
		}
	}
	w.endObject()
	w.endObject()
	return nil
}

// IsKeepalive reports whether this is a keep-alive rather than data.
func (m *DataSetMessage) IsKeepalive() bool {
	return m.MessageType == MessageTypeKeepalive
}

// JSON encodes the message as Part 14 JSON.
func (m *MetaDataMessage) JSON() ([]byte, error) {
	w := &jsonWriter{buf: make([]byte, 0, 256)}
	w.beginObject()
	putStr(w, "MessageId", m.MessageID)
	w.name("MessageType")
	w.str(MessageTypeMetadata)
	putStr(w, "PublisherId", m.PublisherID)
	if m.DataSetWriterID != 0 {
		w.name("DataSetWriterId")
		w.uint(uint64(m.DataSetWriterID))
	}
	putStr(w, "WriterGroupName", m.WriterGroupName)
	putStr(w, "DataSetWriterName", m.DataSetWriterName)
	if !m.Timestamp.IsZero() {
		w.name("Timestamp")
		w.str(formatTicks(m.Timestamp))
	}
	w.name("MetaData")
	if err := putMetaData(w, &m.MetaData); err != nil {
		return nil, codecError("encode a JSON message", err)
	}
	w.endObject()
	return w.buf, nil
}

// object reads a JSON object's members, refusing anything after it.
func object(data []byte) (map[string]json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errors.New("a message is a JSON object, not null")
	}
	return m, nil
}

// text reads a string, or a number written as one (some publishers send a
// numeric PublisherId); null is empty.
func text(raw json.RawMessage) (string, error) {
	v, err := parseJSON(raw)
	if err != nil {
		return "", err
	}
	switch x := v.(type) {
	case nil:
		return "", nil
	case string:
		return x, nil
	case json.Number:
		return string(x), nil
	}
	return "", fmt.Errorf("%s is not a string", raw)
}

// timestamp reads an RFC 3339 string; any other JSON type is absent.
func timestamp(raw json.RawMessage) (time.Time, error) {
	if t := bytes.TrimLeft(raw, " \t\r\n"); len(t) == 0 || t[0] != '"' {
		return time.Time{}, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return time.Time{}, err
	}
	return parseTime(s)
}

// statusOf reads a StatusCode as a number (1.04) or as {"Code": n,
// "Symbol": ...} (1.05) into status; anything else leaves it absent.
func statusOf(raw json.RawMessage, status **ua.StatusCode) error {
	v, err := parseJSON(raw)
	if err != nil {
		return err
	}
	var code any
	switch x := v.(type) {
	case json.Number:
		code = x
	case map[string]any:
		code = x["Code"]
	default:
		return nil
	}
	n, err := uintOf(code, 32)
	if err != nil {
		return err
	}
	s := ua.StatusCode(n)
	*status = &s
	return nil
}

// versionOf reads a ConfigurationVersion object into version; null leaves
// it absent.
func versionOf(raw json.RawMessage, version **ua.ConfigurationVersionDataType) error {
	v, err := parseJSON(raw)
	if err != nil || v == nil {
		return err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return fmt.Errorf("%s is not a ConfigurationVersion", raw)
	}
	major, err := uintOf(m["MajorVersion"], 32)
	if err != nil {
		return fmt.Errorf("member MajorVersion: %w", err)
	}
	minor, err := uintOf(m["MinorVersion"], 32)
	if err != nil {
		return fmt.Errorf("member MinorVersion: %w", err)
	}
	*version = &ua.ConfigurationVersionDataType{MajorVersion: uint32(major), MinorVersion: uint32(minor)}
	return nil
}

// ParseNetworkMessage decodes a Part 14 JSON NetworkMessage, or a
// header-less DataSetMessage or array of them. A message of another
// MessageType, such as ua-metadata, is refused.
func ParseNetworkMessage(data []byte) (NetworkMessage, error) {
	n, err := readNetwork(data)
	if err != nil {
		return NetworkMessage{}, codecError("decode a JSON message", err)
	}
	return n, nil
}

func readNetwork(data []byte) (NetworkMessage, error) {
	var n NetworkMessage
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) == 0 {
		return n, errors.New("the message is empty")
	}
	switch trimmed[0] {
	case '[':
		var items []json.RawMessage
		if err := json.Unmarshal(data, &items); err != nil {
			return n, err
		}
		for _, item := range items {
			m, err := readMessage(item)
			if err != nil {
				return n, err
			}
			n.Messages = append(n.Messages, m)
		}
		return n, nil
	case '{':
	default:
		return n, fmt.Errorf("a NetworkMessage is a JSON object, not %.20s", trimmed)
	}

	// A NetworkMessage, or a DataSetMessage sent without one: the fields of
	// both are read, and which it was decided at the end.
	members, err := object(data)
	if err != nil {
		return n, err
	}
	var kind string
	var lone DataSetMessage
	loneFields := false
	for name, raw := range members {
		var err error
		switch name {
		case "MessageId":
			n.MessageID, err = text(raw)
		case "MessageType":
			kind, err = text(raw)
		case "PublisherId":
			n.PublisherID, err = text(raw)
		case "WriterGroupName", "DataSetWriterGroup":
			// OPC Publisher writes the WriterGroup as DataSetWriterGroup.
			n.WriterGroupName, err = text(raw)
		case "DataSetClassId":
			n.DataSetClassID, err = text(raw)
		case "Messages":
			n.Messages, err = readMessages(raw)
		default:
			var read bool
			read, err = readMessageField(&lone, name, raw)
			loneFields = loneFields || read
		}
		if err != nil {
			return n, fmt.Errorf("%s: %w", name, err)
		}
	}
	switch kind {
	case "", MessageTypeData:
	case "ua-keyframe", "ua-deltaframe", "ua-event", MessageTypeKeepalive:
		if !loneFields {
			return n, fmt.Errorf("this is a %q message, not %q", kind, MessageTypeData)
		}
		lone.MessageType = kind
	default:
		return n, fmt.Errorf("this is a %q message, not %q", kind, MessageTypeData)
	}
	if len(n.Messages) == 0 && loneFields {
		if lone.PublisherID == "" {
			lone.PublisherID = n.PublisherID
		}
		if lone.WriterGroupName == "" {
			lone.WriterGroupName = n.WriterGroupName
		}
		n.Messages = append(n.Messages, lone)
	}
	return n, nil
}

// readMessages reads a Messages value: an array, a lone object, or anything
// else read as none.
func readMessages(raw json.RawMessage) ([]DataSetMessage, error) {
	trimmed := bytes.TrimLeft(raw, " \t\r\n")
	switch {
	case len(trimmed) > 0 && trimmed[0] == '[':
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return nil, err
		}
		out := make([]DataSetMessage, 0, len(items))
		for _, item := range items {
			m, err := readMessage(item)
			if err != nil {
				return nil, err
			}
			out = append(out, m)
		}
		return out, nil
	case len(trimmed) > 0 && trimmed[0] == '{':
		m, err := readMessage(raw)
		if err != nil {
			return nil, err
		}
		return []DataSetMessage{m}, nil
	}
	return nil, nil
}

// readMessage reads one DataSetMessage object, skipping fields this package
// does not read.
func readMessage(raw json.RawMessage) (DataSetMessage, error) {
	var m DataSetMessage
	members, err := object(raw)
	if err != nil {
		return m, err
	}
	for name, value := range members {
		if name == "MessageType" {
			if m.MessageType, err = text(value); err != nil {
				return m, fmt.Errorf("member MessageType: %w", err)
			}
			continue
		}
		if _, err = readMessageField(&m, name, value); err != nil {
			return m, fmt.Errorf("%s: %w", name, err)
		}
	}
	return m, nil
}

// readMessageField reads one DataSetMessage field into m, or reports false
// when name is not one.
func readMessageField(m *DataSetMessage, name string, raw json.RawMessage) (bool, error) {
	var err error
	switch name {
	case "DataSetWriterId":
		var v any
		if v, err = parseJSON(raw); err == nil && v != nil {
			var id uint64
			id, err = uintOf(v, 16)
			m.DataSetWriterID = uint16(id)
		}
	case "DataSetWriterName":
		m.DataSetWriterName, err = text(raw)
	case "SequenceNumber":
		var v any
		if v, err = parseJSON(raw); err == nil && v != nil {
			var seq uint64
			if seq, err = uintOf(v, 32); err == nil {
				s := uint32(seq)
				m.SequenceNumber = &s
			}
		}
	case "MetaDataVersion":
		err = versionOf(raw, &m.MetaDataVersion)
	case "Timestamp":
		m.Timestamp, err = timestamp(raw)
	case "Status":
		err = statusOf(raw, &m.Status)
	case "Payload":
		m.Payload, err = readPayload(raw)
	case "PublisherId":
		// Read only where a DataSetMessage travels without a header; inside
		// one, the header's own fields are what a caller looks at.
		m.PublisherID, err = text(raw)
	case "WriterGroupName", "DataSetWriterGroup":
		m.WriterGroupName, err = text(raw)
	default:
		return false, nil
	}
	return true, err
}

// readPayload reads the Payload object's fields in the order written, each
// as a DataValue.
func readPayload(raw json.RawMessage) ([]Field, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, errors.New("a Payload is a JSON object of fields by name")
	}
	var fields []Field
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		name, _ := tok.(string)
		var value json.RawMessage
		if decodeErr := dec.Decode(&value); decodeErr != nil {
			return nil, decodeErr
		}
		dv, err := fieldValue(value)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", name, err)
		}
		fields = append(fields, Field{Name: name, Value: dv})
	}
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	return fields, nil
}

// ParseMetaDataMessage decodes a Part 14 JSON DataSetMetaData message.
func ParseMetaDataMessage(data []byte) (MetaDataMessage, error) {
	m, err := readMetaDataMessage(data)
	if err != nil {
		return MetaDataMessage{}, codecError("decode a JSON message", err)
	}
	return m, nil
}

func readMetaDataMessage(data []byte) (MetaDataMessage, error) {
	var out MetaDataMessage
	members, err := object(data)
	if err != nil {
		return out, err
	}
	var kind string
	var meta *ua.DataSetMetaDataType
	for name, raw := range members {
		var err error
		switch name {
		case "MessageId":
			out.MessageID, err = text(raw)
		case "MessageType":
			kind, err = text(raw)
		case "PublisherId":
			out.PublisherID, err = text(raw)
		case "DataSetWriterId":
			var v any
			if v, err = parseJSON(raw); err == nil && v != nil {
				var id uint64
				id, err = uintOf(v, 16)
				out.DataSetWriterID = uint16(id)
			}
		case "WriterGroupName", "DataSetWriterGroup":
			out.WriterGroupName, err = text(raw)
		case "DataSetWriterName":
			out.DataSetWriterName, err = text(raw)
		case "Timestamp":
			out.Timestamp, err = timestamp(raw)
		case "MetaData":
			meta, err = metaDataOf(raw)
		}
		if err != nil {
			return out, fmt.Errorf("%s: %w", name, err)
		}
	}
	if kind != MessageTypeMetadata {
		return out, fmt.Errorf("the MessageType is %q, not %q", kind, MessageTypeMetadata)
	}
	if meta == nil {
		return out, errors.New("the message carries no MetaData")
	}
	out.MetaData = *meta
	return out, nil
}
