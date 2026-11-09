package pubsub

import (
	"fmt"
	"reflect"

	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
)

// CodecName is the codec every delivered message carries: a DataValue in OPC
// UA binary encoding (Part 6, 5.2.2.17), named as runtime-rs names it.
const CodecName = "opcua.datavalue"

// DataValueCodec decodes what a subscription delivers: each message's data
// is one field's DataValue in OPC UA binary, exactly a DataValue's encoding
// on an OPC UA wire, with nothing of this module's frame added. So the
// status code and both timestamps reach the subscriber beside the value.
//
// [streams.Message.Decode] uses it on its own, because a message carries its
// codec. It decodes into a *ua.DataValue (everything), a *ua.Variant (the
// value), or a pointer to the value's own Go type — a *float64 for a Double,
// a *time.Time for a DateTime — or to an any.
//
// It encodes the values [DataValueOf] accepts, for a program that wants the
// same bytes itself.
var DataValueCodec streams.Codec = dataValueCodec{}

type dataValueCodec struct{}

func (dataValueCodec) Name() string { return CodecName }

func (dataValueCodec) Marshal(v any) ([]byte, error) {
	dv, err := DataValueOf(v)
	if err != nil {
		return nil, err
	}
	return encodeDataValue(dv)
}

func (dataValueCodec) Unmarshal(data []byte, dest any) error {
	dv, err := decodeDataValue(data)
	if err != nil {
		return err
	}
	var value any
	if dv.Value != nil {
		value = dv.Value.Value()
	}
	switch d := dest.(type) {
	case nil:
		return fmt.Errorf("opcua pubsub: decode destination is nil")
	case *ua.DataValue:
		*d = *dv
		return nil
	case **ua.DataValue:
		*d = dv
		return nil
	case *ua.Variant:
		if dv.Value == nil {
			*d = ua.Variant{}
		} else {
			*d = *dv.Value
		}
		return nil
	case *any:
		*d = value
		return nil
	}
	target := reflect.ValueOf(dest)
	if target.Kind() != reflect.Pointer || target.IsNil() {
		return fmt.Errorf("opcua pubsub: decode destination is a %T, not a non-nil pointer", dest)
	}
	if value == nil {
		return fmt.Errorf("opcua pubsub: the DataValue holds no value to decode into %T", dest)
	}
	got := reflect.ValueOf(value)
	if !got.Type().AssignableTo(target.Elem().Type()) {
		return fmt.Errorf("opcua pubsub: the value is a %T, which does not decode into %T; decode into a *ua.DataValue or *any to take whatever arrives", value, dest)
	}
	target.Elem().Set(got)
	return nil
}

// DataValueOf is the DataValue a published field carries: a *ua.DataValue
// or ua.DataValue as it is, a *ua.Variant as its value, or any value gopcua
// holds as a Variant — bool, the sized integers, float32 and float64,
// string, []byte, time.Time, ua.StatusCode, *ua.NodeID and the rest, and
// slices of them — as a DataValue holding only that value, so the
// subscriber's Variant has exactly that type.
//
// Anything else is refused with an error wrapping [streams.ErrUnsupported]:
// a field is a typed OPC UA value, and a struct or Go's int has no type to
// carry it. Go's int is refused rather than guessed at, as the opcua package
// refuses it; pass the width the reader expects, such as int32.
func DataValueOf(v any) (*ua.DataValue, error) {
	switch x := v.(type) {
	case *ua.DataValue:
		if x == nil {
			return &ua.DataValue{}, nil
		}
		return x, nil
	case ua.DataValue:
		return &x, nil
	case *ua.Variant:
		return &ua.DataValue{EncodingMask: ua.DataValueValue, Value: x}, nil
	case int, uint, []int, []uint:
		return nil, fmt.Errorf("%w: opcua pubsub: OPC UA has no type for Go's %T; "+
			"pass the width the field holds, such as int32 for an Int32", streams.ErrUnsupported, v)
	case DataSet, *DataSet:
		return nil, fmt.Errorf("%w: opcua pubsub: a field's subject takes one OPC UA value, "+
			"and this is a whole DataSet; publish it to the writer's subject", streams.ErrUnsupported)
	}
	if v != nil && isOPCUA(reflect.TypeOf(v)) {
		variant, err := ua.NewVariant(v)
		if err == nil {
			return &ua.DataValue{EncodingMask: ua.DataValueValue, Value: variant}, nil
		}
	}
	return nil, fmt.Errorf("%w: opcua pubsub: a field is a typed OPC UA value, and a %T is not one; "+
		"pass a *ua.DataValue, a *ua.Variant, or a value gopcua holds as a Variant", streams.ErrUnsupported, v)
}

// isOPCUA reports whether t is a type gopcua writes as a built-in type, or a
// slice of one. gopcua accepts a slice of anything as an array of Variants,
// which would carry a struct nobody could read, so this is the stricter
// check.
func isOPCUA(t reflect.Type) bool {
	for t.Kind() == reflect.Slice && t != reflect.TypeOf([]byte(nil)) {
		t = t.Elem()
	}
	switch t {
	case reflect.TypeOf(false), reflect.TypeOf(int8(0)), reflect.TypeOf(uint8(0)),
		reflect.TypeOf(int16(0)), reflect.TypeOf(uint16(0)), reflect.TypeOf(int32(0)),
		reflect.TypeOf(uint32(0)), reflect.TypeOf(int64(0)), reflect.TypeOf(uint64(0)),
		reflect.TypeOf(float32(0)), reflect.TypeOf(float64(0)), reflect.TypeOf(""),
		reflect.TypeOf([]byte(nil)), elemType(ua.TypeIDDateTime), elemType(ua.TypeIDGUID),
		elemType(ua.TypeIDXMLElement), elemType(ua.TypeIDNodeID), elemType(ua.TypeIDExpandedNodeID),
		elemType(ua.TypeIDStatusCode), elemType(ua.TypeIDQualifiedName), elemType(ua.TypeIDLocalizedText),
		elemType(ua.TypeIDDataValue), elemType(ua.TypeIDVariant), reflect.TypeOf(ua.ByteArray(nil)):
		return true
	}
	return false
}

// encodeDataValue is a DataValue's OPC UA binary encoding, its mask computed
// from what it holds.
func encodeDataValue(dv *ua.DataValue) ([]byte, error) {
	out := *dv
	out.EncodingMask = presence(dv)
	b, err := out.Encode()
	if err != nil {
		return nil, fmt.Errorf("opcua pubsub: cannot encode an OPC UA DataValue: %w", err)
	}
	return b, nil
}

// decodeDataValue reads one DataValue, refusing trailing bytes: a payload is
// one value, and anything after it means the bytes are not what they claim.
func decodeDataValue(data []byte) (dv *ua.DataValue, err error) {
	defer func() {
		// gopcua's decoder trusts lengths it reads; a corrupt one is an error
		// here, not a crash.
		if r := recover(); r != nil {
			dv, err = nil, fmt.Errorf("opcua pubsub: cannot decode an OPC UA DataValue: %v", r)
		}
	}()
	dv = &ua.DataValue{}
	n, err := dv.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("opcua pubsub: cannot decode an OPC UA DataValue: %w", err)
	}
	if n != len(data) {
		return nil, fmt.Errorf("opcua pubsub: %d bytes follow the DataValue", len(data)-n)
	}
	if dv.EncodingMask&ua.DataValueValue == 0 {
		dv.Value = nil
	}
	return dv, nil
}
