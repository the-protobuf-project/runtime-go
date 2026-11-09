package pubsub

import (
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"

	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
)

// DatasetCodecName names a [DataSet]'s binary form, as runtime-rs names it.
const DatasetCodecName = "opcua.dataset"

// maxFields is the most fields a DataSet's binary form may declare, so a
// corrupt count cannot make a reader allocate without bound.
const maxFields = 1 << 16

// DataSet is a DataSet's fields by name, each a DataValue, published as one
// DataSetMessage to a writer's subject.
//
// It is only the publish side's container; a subscriber receives each field
// as a message of its own.
type DataSet struct {
	// Fields are the fields, in order.
	Fields []Field

	// err is the first value With could not take, reported when the set is
	// published or encoded, so a DataSet still builds in one expression.
	err error
}

// NewDataSet returns an empty DataSet.
func NewDataSet() DataSet { return DataSet{} }

// With returns d with a field added. value is anything [DataValueOf] takes;
// one it refuses fails the publish that carries the set, naming the field.
func (d DataSet) With(name string, value any) DataSet {
	dv, err := DataValueOf(value)
	if err != nil {
		if d.err == nil {
			d.err = fmt.Errorf("field %q: %w", name, err)
		}
		return d
	}
	d.Fields = append(d.Fields[:len(d.Fields):len(d.Fields)], Field{Name: name, Value: dv})
	return d
}

// Err is the first value [DataSet.With] refused, or nil.
func (d DataSet) Err() error { return d.err }

// MarshalBinary is the DataSet in OPC UA binary, the same bytes runtime-rs's
// DataSet payload holds: an Int32 count, then each field's name as a String
// and its DataValue.
func (d DataSet) MarshalBinary() ([]byte, error) {
	if d.err != nil {
		return nil, d.err
	}
	out := binary.LittleEndian.AppendUint32(make([]byte, 0, 4+len(d.Fields)*24), uint32(len(d.Fields)))
	for _, f := range d.Fields {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(f.Name)))
		out = append(out, f.Name...)
		dv := f.Value
		if dv == nil {
			dv = &ua.DataValue{}
		}
		b, err := encodeDataValue(dv)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", f.Name, err)
		}
		out = append(out, b...)
	}
	return out, nil
}

// UnmarshalBinary reads a DataSet's binary form, refusing a truncated or
// corrupt one and bytes after the last field.
func (d *DataSet) UnmarshalBinary(data []byte) error {
	bad := func(why any) error {
		return fmt.Errorf("opcua pubsub: cannot decode a DataSet: %v", why)
	}
	if len(data) < 4 {
		return bad("it is truncated")
	}
	count := int32(binary.LittleEndian.Uint32(data))
	if count < 0 {
		return bad("it declares a negative count")
	}
	if count > maxFields {
		return bad(fmt.Sprintf("it declares %d fields", count))
	}
	rest := data[4:]
	fields := make([]Field, 0, count)
	for i := int32(0); i < count; i++ {
		if len(rest) < 4 {
			return bad("it is truncated")
		}
		n := int32(binary.LittleEndian.Uint32(rest))
		rest = rest[4:]
		var name string
		switch {
		case n == -1:
		case n < 0 || int(n) > len(rest):
			return bad("a field name runs past the end")
		default:
			name = string(rest[:n])
			rest = rest[n:]
		}
		dv, used, err := decodeDataValuePrefix(rest)
		if err != nil {
			return bad(err)
		}
		rest = rest[used:]
		fields = append(fields, Field{Name: name, Value: dv})
	}
	if len(rest) != 0 {
		return bad("bytes follow the last field")
	}
	d.Fields, d.err = fields, nil
	return nil
}

// decodeDataValuePrefix reads one DataValue from the front of data and says
// how many bytes it took.
func decodeDataValuePrefix(data []byte) (dv *ua.DataValue, n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			dv, n, err = nil, 0, fmt.Errorf("%v", r)
		}
	}()
	if len(data) == 0 {
		return nil, 0, errors.New("it is truncated")
	}
	dv = &ua.DataValue{}
	n, err = dv.Decode(data)
	if err != nil {
		return nil, 0, err
	}
	if dv.EncodingMask&ua.DataValueValue == 0 {
		dv.Value = nil
	}
	return dv, n, nil
}

// Metadata is DataSetMetaData describing these fields: each one's name,
// built-in type, and whether it is a scalar or an array, read off its value.
// An empty value is described as BaseDataType. The ConfigurationVersion is
// left zero, for [Provider.PublishMetadata] to stamp.
func (d DataSet) Metadata(name string) ua.DataSetMetaDataType {
	fields := make([]*ua.FieldMetaData, 0, len(d.Fields))
	for _, f := range d.Fields {
		var (
			builtIn uint32
			rank    int32 = -2
			dims    []uint32
		)
		if f.Value != nil && f.Value.Value != nil && f.Value.Value.Value() != nil {
			builtIn = uint32(f.Value.Value.Type())
			rank = -1
			if v := reflect.ValueOf(f.Value.Value.Value()); isArray(v) {
				var flat []reflect.Value
				shape := flatten(v, &flat)
				rank = 1
				if len(shape) > 1 {
					rank = int32(len(shape))
					for _, s := range shape {
						dims = append(dims, uint32(s))
					}
				}
			}
		}
		// A built-in type's DataType node is its own number in namespace 0;
		// BaseDataType is 24.
		dataType := builtIn
		if dataType == 0 {
			dataType = 24
		}
		fields = append(fields, &ua.FieldMetaData{
			Name:            f.Name,
			BuiltInType:     uint8(builtIn),
			DataType:        numericNodeID(0, dataType),
			ValueRank:       rank,
			ArrayDimensions: dims,
		})
	}
	return ua.DataSetMetaDataType{
		Name:                 name,
		Fields:               fields,
		ConfigurationVersion: &ua.ConfigurationVersionDataType{},
	}
}

// dataSetOf is the DataSet a writer's subject publishes, refusing anything
// else by name.
func dataSetOf(v any) (DataSet, error) {
	switch x := v.(type) {
	case DataSet:
		return x, x.err
	case *DataSet:
		if x != nil {
			return *x, x.err
		}
	}
	return DataSet{}, fmt.Errorf("%w: opcua pubsub: a DataSetWriter's subject takes a whole DataSet, "+
		"and this is a %T; build it with NewDataSet, or publish one field to \"<writer>/<field>\"",
		streams.ErrUnsupported, v)
}
