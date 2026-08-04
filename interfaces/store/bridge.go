package store

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// bridge.go is the single reflection layer every driver reuses. It moves values
// between a proto.Message and a backend column map (keyed by Column.Name), using
// the Resource descriptor to pair proto fields with columns. Values are
// normalized to a small, predictable set of Go types so a relational driver, an
// ABI encoder, or a GraphQL writer all see the same shapes:
//
//	KindString    -> string        KindBytes     -> []byte
//	KindInt       -> int64         KindFloat     -> float64
//	KindUint      -> uint64        KindTimestamp -> time.Time
//	KindBool      -> bool          KindEnum      -> int32 (enum number)
//
// A column whose proto field supports presence and is unset becomes nil, so a
// backend can store NULL and ColumnsToMessage leaves it unset on the way back.

const timestampFullName = "google.protobuf.Timestamp"

// MessageToColumns reads every column value out of msg by its proto field,
// returning a map keyed by backend column name. An unset optional field is
// stored as nil.
func MessageToColumns(res *Resource, msg proto.Message) (map[string]any, error) {
	m := msg.ProtoReflect()
	fields := m.Descriptor().Fields()
	out := make(map[string]any, len(res.Columns))
	for _, col := range res.Columns {
		// Driver-managed columns (a generated key, an audit timestamp) have no
		// proto field; the driver supplies their value, not the message.
		if col.Field == "" {
			continue
		}
		fd := fields.ByName(col.Field)
		if fd == nil {
			return nil, fmt.Errorf("store: resource %q column %q: no proto field %q", res.Name, col.Name, col.Field)
		}
		// Treat an unset presence-bearing field as NULL, unless the column is
		// required (then we store the zero value to satisfy NOT NULL).
		if fd.HasPresence() && !m.Has(fd) && !col.NotNull {
			out[col.Name] = nil
			continue
		}
		v, err := getValue(col.Kind, fd, m.Get(fd))
		if err != nil {
			return nil, fmt.Errorf("store: resource %q column %q: %w", res.Name, col.Name, err)
		}
		out[col.Name] = v
	}
	return out, nil
}

// ColumnsToMessage builds a new message of res's type and populates each field
// from row (keyed by backend column name). The inverse of MessageToColumns; a
// nil or absent value leaves the field unset.
func ColumnsToMessage(res *Resource, row map[string]any) (proto.Message, error) {
	if res.New == nil {
		return nil, fmt.Errorf("store: resource %q has no New constructor", res.Name)
	}
	msg := res.New()
	m := msg.ProtoReflect()
	fields := m.Descriptor().Fields()
	for _, col := range res.Columns {
		if col.Field == "" {
			continue
		}
		val, ok := row[col.Name]
		if !ok || val == nil {
			continue
		}
		fd := fields.ByName(col.Field)
		if fd == nil {
			return nil, fmt.Errorf("store: resource %q column %q: no proto field %q", res.Name, col.Name, col.Field)
		}
		v, err := protoValue(col.Kind, fd, val)
		if err != nil {
			return nil, fmt.Errorf("store: resource %q column %q: %w", res.Name, col.Name, err)
		}
		m.Set(fd, v)
	}
	return msg, nil
}

// KeyOf returns the string primary-key value of msg, the lookup key drivers and
// the adapter use for Get/Update/Delete.
func KeyOf(res *Resource, msg proto.Message) (string, error) {
	pk, ok := res.PK()
	if !ok {
		return "", fmt.Errorf("store: resource %q has no primary key", res.Name)
	}
	m := msg.ProtoReflect()
	fd := m.Descriptor().Fields().ByName(pk.Field)
	if fd == nil {
		return "", fmt.Errorf("store: resource %q: no proto field %q for primary key", res.Name, pk.Field)
	}
	return m.Get(fd).String(), nil
}

// getValue normalizes a proto field value to the Go type for its Kind.
func getValue(kind Kind, fd protoreflect.FieldDescriptor, v protoreflect.Value) (any, error) {
	switch kind {
	case KindTimestamp:
		if fd.Kind() != protoreflect.MessageKind || fd.Message().FullName() != timestampFullName {
			return nil, fmt.Errorf("KindTimestamp on non-Timestamp field %q", fd.Name())
		}
		// Read seconds/nanos through protoreflect rather than asserting a concrete
		// *timestamppb.Timestamp, so the bridge works for both generated messages
		// and dynamicpb (where the value is a *dynamicpb.Message).
		tm := v.Message()
		secFD := tm.Descriptor().Fields().ByName("seconds")
		nanoFD := tm.Descriptor().Fields().ByName("nanos")
		return time.Unix(tm.Get(secFD).Int(), tm.Get(nanoFD).Int()).UTC(), nil
	case KindEnum:
		return int32(v.Enum()), nil
	case KindInt:
		return v.Int(), nil
	case KindUint:
		return v.Uint(), nil
	case KindFloat:
		return v.Float(), nil
	case KindBool:
		return v.Bool(), nil
	case KindBytes:
		return v.Bytes(), nil
	case KindString, KindUnknown:
		return v.String(), nil
	default:
		return v.Interface(), nil
	}
}

// protoValue coerces a normalized Go value back into a protoreflect.Value
// matching fd's concrete kind (so a backend that returns int64 for an int32
// column, or a string for an enum, still sets correctly).
func protoValue(kind Kind, fd protoreflect.FieldDescriptor, val any) (protoreflect.Value, error) {
	switch kind {
	case KindTimestamp:
		t, err := toTime(val)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfMessage(timestamppb.New(t).ProtoReflect()), nil
	case KindEnum:
		n, err := toInt64(val)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(n)), nil
	}
	switch fd.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(fmt.Sprint(val)), nil
	case protoreflect.BoolKind:
		b, err := toBool(val)
		return protoreflect.ValueOfBool(b), err
	case protoreflect.BytesKind:
		b, err := toBytes(val)
		return protoreflect.ValueOfBytes(b), err
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		n, err := toInt64(val)
		return protoreflect.ValueOfInt32(int32(n)), err
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		n, err := toInt64(val)
		return protoreflect.ValueOfInt64(n), err
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		n, err := toUint64(val)
		return protoreflect.ValueOfUint32(uint32(n)), err
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		n, err := toUint64(val)
		return protoreflect.ValueOfUint64(n), err
	case protoreflect.FloatKind:
		f, err := toFloat64(val)
		return protoreflect.ValueOfFloat32(float32(f)), err
	case protoreflect.DoubleKind:
		f, err := toFloat64(val)
		return protoreflect.ValueOfFloat64(f), err
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported proto kind %s for field %q", fd.Kind(), fd.Name())
	}
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case int32:
		return int64(x), nil
	case int:
		return int64(x), nil
	case uint64:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case float64:
		return int64(x), nil
	case float32:
		return int64(x), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toUint64(v any) (uint64, error) {
	switch x := v.(type) {
	case uint64:
		return x, nil
	case uint32:
		return uint64(x), nil
	case int64:
		return uint64(x), nil
	case int32:
		return uint64(x), nil
	case int:
		return uint64(x), nil
	case float64:
		return uint64(x), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to uint64", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case int:
		return float64(x), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

func toBool(v any) (bool, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case int64:
		return x != 0, nil
	case int:
		return x != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", v)
	}
}

func toBytes(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to []byte", v)
	}
}

func toTime(v any) (time.Time, error) {
	switch x := v.(type) {
	case time.Time:
		return x, nil
	case int64:
		return time.Unix(x, 0).UTC(), nil
	case string:
		return time.Parse(time.RFC3339, x)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", v)
	}
}
