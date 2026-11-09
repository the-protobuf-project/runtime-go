package pubsub

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gopcua/opcua/ua"
)

// This file is Part 6's JSON encoding (5.4) of the values a DataSetMessage
// carries, in the reversible form async-opcua writes: a Variant is
// {"Type":<built-in type>,"Body":<value>}, so its type survives the wire, and
// a DataValue is an object of Value, Status, timestamps and picoseconds, each
// left out when absent. The bytes match runtime-rs's for every built-in type
// gopcua holds, which is what lets the two providers read each other.

// opcuaEpoch is where an OPC UA DateTime starts counting; earlier times are
// clipped to it, as runtime-rs clips them, and the zero time is it.
var opcuaEpoch = time.Date(1601, 1, 1, 0, 0, 0, 0, time.UTC)

// opcuaEnd is the last time an OPC UA DateTime holds.
var opcuaEnd = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

// putVariant writes v as a reversible Variant, or null when it holds nothing.
func putVariant(w *jsonWriter, v *ua.Variant) error {
	if v == nil || v.Value() == nil || v.Type() == ua.TypeIDNull {
		w.null()
		return nil
	}
	w.beginObject()
	w.name("Type")
	w.uint(uint64(v.Type()))
	value := reflect.ValueOf(v.Value())
	if isArray(value) {
		var flat []reflect.Value
		dims := flatten(value, &flat)
		if len(dims) > 1 {
			w.name("Dimensions")
			w.beginArray()
			for _, d := range dims {
				w.int(int64(d))
			}
			w.endArray()
		}
		w.name("Body")
		w.beginArray()
		for _, item := range flat {
			if err := putBody(w, v.Type(), item.Interface()); err != nil {
				return err
			}
		}
		w.endArray()
	} else {
		w.name("Body")
		if err := putBody(w, v.Type(), v.Value()); err != nil {
			return err
		}
	}
	w.endObject()
	return nil
}

// isArray reports whether a variant's Go value is a slice of values rather
// than a ByteString, which is a []byte holding one value.
func isArray(v reflect.Value) bool {
	if v.Kind() != reflect.Slice {
		return false
	}
	return v.Type() != reflect.TypeOf([]byte(nil))
}

// flatten appends the elements of a possibly nested slice to out, in row
// order, and returns its dimensions.
func flatten(v reflect.Value, out *[]reflect.Value) []int {
	if !isArray(v) {
		*out = append(*out, v)
		return nil
	}
	var inner []int
	for i := 0; i < v.Len(); i++ {
		d := flatten(v.Index(i), out)
		if i == 0 {
			inner = d
		}
	}
	return append([]int{v.Len()}, inner...)
}

// putBody writes one value of a Variant's body: a scalar, or one element of
// an array.
func putBody(w *jsonWriter, t ua.TypeID, value any) error {
	switch x := value.(type) {
	case nil:
		w.null()
	case bool:
		w.boolean(x)
	case int8:
		w.int(int64(x))
	case int16:
		w.int(int64(x))
	case int32:
		w.int(int64(x))
	case int64:
		w.int(x)
	case uint8:
		w.uint(uint64(x))
	case uint16:
		w.uint(uint64(x))
	case uint32:
		w.uint(uint64(x))
	case uint64:
		w.uint(x)
	case float32:
		putFloat(w, float64(x), 32)
	case float64:
		putFloat(w, x, 64)
	case string:
		w.str(x)
	case ua.XMLElement:
		w.str(string(x))
	case time.Time:
		w.str(formatMillis(x))
	case *ua.GUID:
		if x == nil {
			w.null()
		} else {
			w.str(strings.ToLower(x.String()))
		}
	case []byte:
		if x == nil {
			w.null()
		} else {
			w.str(base64.StdEncoding.EncodeToString(x))
		}
	case ua.StatusCode:
		w.uint(uint64(x))
	case *ua.QualifiedName:
		switch {
		case x == nil || x.Name == "" && x.NamespaceIndex == 0:
			w.null()
		case x.NamespaceIndex > 0:
			w.str(strconv.Itoa(int(x.NamespaceIndex)) + ":" + x.Name)
		default:
			w.str(x.Name)
		}
	case *ua.LocalizedText:
		w.beginObject()
		if x != nil {
			if x.Locale != "" {
				w.name("Locale")
				w.str(x.Locale)
			}
			if x.Text != "" {
				w.name("Text")
				w.str(x.Text)
			}
		}
		w.endObject()
	case *ua.NodeID:
		putNodeID(w, x, "", 0)
	case *ua.ExpandedNodeID:
		if x == nil {
			w.null()
		} else {
			putNodeID(w, x.NodeID, x.NamespaceURI, x.ServerIndex)
		}
	case *ua.DataValue:
		return putDataValue(w, x, formatMillis)
	case *ua.Variant:
		return putVariant(w, x)
	default:
		return fmt.Errorf("opcua pubsub: cannot write a %v (%T) as Part 6 JSON", t, value)
	}
	return nil
}

// putFloat writes a float as Rust's Display does, which is what async-opcua
// writes: the shortest digits that read back exactly, never an exponent, and
// the three non-finite values as Part 6's strings.
func putFloat(w *jsonWriter, f float64, bits int) {
	switch {
	case math.IsNaN(f):
		w.str("NaN")
	case math.IsInf(f, 1):
		w.str("Infinity")
	case math.IsInf(f, -1):
		w.str("-Infinity")
	default:
		w.raw(strconv.FormatFloat(f, 'f', -1, bits))
	}
}

// putNodeID writes a NodeId as Part 6 does: a numeric id alone, other kinds
// with their IdType, then the namespace when it is not 0. A null id is
// written as the numeric id 0, as async-opcua writes one.
func putNodeID(w *jsonWriter, n *ua.NodeID, uri string, server uint32) {
	w.beginObject()
	switch {
	case n == nil:
		w.name("Id")
		w.uint(0)
	case n.Type() == ua.NodeIDTypeString:
		w.name("IdType")
		w.uint(1)
		w.name("Id")
		w.str(n.StringID())
	case n.Type() == ua.NodeIDTypeGUID:
		w.name("IdType")
		w.uint(2)
		w.name("Id")
		w.str(strings.ToLower(n.StringID()))
	case n.Type() == ua.NodeIDTypeByteString:
		w.name("IdType")
		w.uint(3)
		w.name("Id")
		w.str(n.StringID())
	default:
		w.name("Id")
		w.uint(uint64(n.IntID()))
	}
	if uri != "" {
		w.name("Namespace")
		w.str(uri)
	} else if n != nil && n.Namespace() != 0 {
		w.name("Namespace")
		w.uint(uint64(n.Namespace()))
	}
	if server != 0 {
		w.name("ServerUri")
		w.uint(uint64(server))
	}
	w.endObject()
}

// opcuaTime clips t to the range an OPC UA DateTime holds, to the 100
// nanoseconds it counts in; the zero time is OPC UA's null DateTime.
func opcuaTime(t time.Time) time.Time {
	t = t.UTC()
	switch {
	case t.IsZero() || t.Before(opcuaEpoch):
		return opcuaEpoch
	case t.After(opcuaEnd):
		return opcuaEnd
	}
	return t.Truncate(100 * time.Nanosecond)
}

// formatMillis is a DateTime as async-opcua writes one inside a Variant:
// RFC 3339 to the millisecond, always three digits.
func formatMillis(t time.Time) string {
	return opcuaTime(t).Format("2006-01-02T15:04:05.000Z")
}

// formatTicks is a DateTime as runtime-rs writes the timestamps of a
// NetworkMessage and its DataValues, to the 100 ns an OPC UA DateTime holds:
// the fraction is left off when it is zero and otherwise written in 3, 6 or
// 9 digits, as chrono's %.f does.
func formatTicks(t time.Time) string {
	t = opcuaTime(t)
	base := t.Format("2006-01-02T15:04:05")
	ns := t.Nanosecond()
	switch {
	case ns == 0:
		return base + "Z"
	case ns%1_000_000 == 0:
		return fmt.Sprintf("%s.%03dZ", base, ns/1_000_000)
	case ns%1_000 == 0:
		return fmt.Sprintf("%s.%06dZ", base, ns/1_000)
	default:
		return fmt.Sprintf("%s.%09dZ", base, ns)
	}
}

// parseTime reads an RFC 3339 time into the range an OPC UA DateTime holds.
// The epoch reads as the zero time, which is how gopcua holds a null one.
func parseTime(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, err
	}
	t = opcuaTime(t)
	if t.Equal(opcuaEpoch) {
		return time.Time{}, nil
	}
	return t, nil
}

// presence is which parts of a DataValue are set: its encoding mask where
// one was set, and otherwise whichever fields hold something, so a DataValue
// built by hand without a mask still says what it carries.
func presence(dv *ua.DataValue) byte {
	mask := dv.EncodingMask
	if dv.Value != nil && dv.Value.Value() != nil {
		mask |= ua.DataValueValue
	} else {
		mask &^= ua.DataValueValue
	}
	if dv.Status != 0 {
		mask |= ua.DataValueStatusCode
	}
	if !dv.SourceTimestamp.IsZero() {
		mask |= ua.DataValueSourceTimestamp
	}
	if !dv.ServerTimestamp.IsZero() {
		mask |= ua.DataValueServerTimestamp
	}
	if dv.SourcePicoseconds != 0 {
		mask |= ua.DataValueSourcePicoseconds
	}
	if dv.ServerPicoseconds != 0 {
		mask |= ua.DataValueServerPicoseconds
	}
	return mask
}

// putDataValue writes a DataValue in Part 6's reversible JSON form
// (5.4.2.18), each part only when it is present, timestamps formatted by
// stamp.
func putDataValue(w *jsonWriter, dv *ua.DataValue, stamp func(time.Time) string) error {
	w.beginObject()
	if dv != nil {
		mask := presence(dv)
		if mask&ua.DataValueValue != 0 {
			w.name("Value")
			if err := putVariant(w, dv.Value); err != nil {
				return err
			}
		}
		if mask&ua.DataValueStatusCode != 0 {
			w.name("Status")
			w.uint(uint64(dv.Status))
		}
		if mask&ua.DataValueSourceTimestamp != 0 {
			w.name("SourceTimestamp")
			w.str(stamp(dv.SourceTimestamp))
		}
		if mask&ua.DataValueSourcePicoseconds != 0 {
			w.name("SourcePicoseconds")
			w.uint(uint64(dv.SourcePicoseconds))
		}
		if mask&ua.DataValueServerTimestamp != 0 {
			w.name("ServerTimestamp")
			w.str(stamp(dv.ServerTimestamp))
		}
		if mask&ua.DataValueServerPicoseconds != 0 {
			w.name("ServerPicoseconds")
			w.uint(uint64(dv.ServerPicoseconds))
		}
	}
	w.endObject()
	return nil
}

// Reading. Other publishers choose other forms, so a Payload field is read in
// any of the forms runtime-rs reads: a reversible DataValue, a bare Variant,
// the 1.05 forms that carry the type as UaType, or a plain JSON value from a
// non-reversible encoder, whose type is then inferred.

// parseJSON reads one JSON value with numbers kept as written, refusing
// anything after it.
func parseJSON(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("bytes follow the value")
	}
	return v, nil
}

// fieldValue reads one Payload field as a DataValue, in whichever form it
// was written.
func fieldValue(raw []byte) (*ua.DataValue, error) {
	v, err := parseJSON(raw)
	if err != nil {
		return nil, err
	}
	return dataValueOf(v)
}

// dataValueKeys are the members that make an object a DataValue.
var dataValueKeys = []string{
	"Value", "Status", "StatusCode", "SourceTimestamp", "SourcePicoseconds",
	"ServerTimestamp", "ServerPicoseconds",
}

// dataValueOf turns a parsed field into a DataValue.
func dataValueOf(v any) (*ua.DataValue, error) {
	dv := &ua.DataValue{}
	m, ok := v.(map[string]any)
	if !ok {
		variant, err := infer(v)
		if err != nil {
			return nil, err
		}
		setValue(dv, variant)
		return dv, nil
	}

	_, hasValue := m["Value"]
	_, hasType := m["Type"]
	switch {
	case m["UaType"] != nil:
		// 1.05: the Variant's type and value inline, beside the DataValue's.
		variant, err := variantOf(m["UaType"], m["Value"], m["Dimensions"])
		if err != nil {
			return nil, err
		}
		setValue(dv, variant)
	case hasType && !hasValue:
		variant, err := variantOf(m["Type"], m["Body"], m["Dimensions"])
		if err != nil {
			return nil, err
		}
		setValue(dv, variant)
		return dv, nil
	case len(m) > 0 && !hasAny(m, dataValueKeys):
		return nil, errors.New("the field is a structure, not a DataValue or Variant")
	case hasValue:
		var (
			variant *ua.Variant
			err     error
		)
		switch inner := m["Value"].(type) {
		case map[string]any:
			switch {
			case inner["UaType"] != nil:
				variant, err = variantOf(inner["UaType"], inner["Value"], inner["Dimensions"])
			case inner["Type"] != nil:
				variant, err = variantOf(inner["Type"], inner["Body"], inner["Dimensions"])
			default:
				variant, err = infer(inner)
			}
		default:
			variant, err = infer(inner)
		}
		if err != nil {
			return nil, err
		}
		setValue(dv, variant)
	}

	code := m["Status"]
	if code == nil {
		code = m["StatusCode"]
	}
	switch c := code.(type) {
	case map[string]any:
		n, err := uintOf(c["Code"], 32)
		if err != nil && c["Code"] != nil {
			return nil, fmt.Errorf("member Status: %w", err)
		}
		dv.Status = ua.StatusCode(n)
		dv.EncodingMask |= ua.DataValueStatusCode
	case json.Number, string:
		n, err := uintOf(c, 32)
		if err != nil {
			return nil, fmt.Errorf("member Status: %w", err)
		}
		dv.Status = ua.StatusCode(n)
		dv.EncodingMask |= ua.DataValueStatusCode
	}
	for _, ts := range []struct {
		key  string
		bit  byte
		into *time.Time
	}{
		{"SourceTimestamp", ua.DataValueSourceTimestamp, &dv.SourceTimestamp},
		{"ServerTimestamp", ua.DataValueServerTimestamp, &dv.ServerTimestamp},
	} {
		if s, ok := m[ts.key].(string); ok {
			t, err := parseTime(s)
			if err != nil {
				return nil, fmt.Errorf("member %s: %w", ts.key, err)
			}
			*ts.into = t
			dv.EncodingMask |= ts.bit
		}
	}
	for _, ps := range []struct {
		key  string
		bit  byte
		into *uint16
	}{
		{"SourcePicoseconds", ua.DataValueSourcePicoseconds, &dv.SourcePicoseconds},
		{"ServerPicoseconds", ua.DataValueServerPicoseconds, &dv.ServerPicoseconds},
	} {
		if p, ok := m[ps.key]; ok && p != nil {
			n, err := uintOf(p, 16)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", ps.key, err)
			}
			*ps.into = uint16(n)
			dv.EncodingMask |= ps.bit
		}
	}
	return dv, nil
}

// setValue puts a variant in a DataValue, marking it present unless it holds
// nothing.
func setValue(dv *ua.DataValue, v *ua.Variant) {
	if v == nil || v.Value() == nil {
		return
	}
	dv.Value = v
	dv.EncodingMask |= ua.DataValueValue
}

func hasAny(m map[string]any, keys []string) bool {
	for _, k := range keys {
		if _, ok := m[k]; ok {
			return true
		}
	}
	return false
}

// infer types a plain JSON value from a non-reversible encoder: Boolean,
// Int64 (UInt64 past its range), Double, or String, and arrays of one of
// them, as runtime-rs does. Null is no value.
func infer(v any) (*ua.Variant, error) {
	typeOf := func(v any) (ua.TypeID, error) {
		switch x := v.(type) {
		case bool:
			return ua.TypeIDBoolean, nil
		case json.Number:
			s := string(x)
			if _, err := strconv.ParseInt(s, 10, 64); err == nil {
				return ua.TypeIDInt64, nil
			}
			if _, err := strconv.ParseUint(s, 10, 64); err == nil {
				return ua.TypeIDUint64, nil
			}
			return ua.TypeIDDouble, nil
		case string:
			return ua.TypeIDString, nil
		}
		return 0, errors.New("a nested array or structure has no inferable type")
	}
	switch x := v.(type) {
	case nil:
		return ua.MustVariant(nil), nil
	case []any:
		var t ua.TypeID
		for _, item := range x {
			this, err := typeOf(item)
			if err != nil {
				return nil, err
			}
			switch {
			case t == 0 || t == this:
				t = this
			case numeric(t) && numeric(this):
				// Integers and fractions in one array are all Double.
				t = ua.TypeIDDouble
			default:
				return nil, errors.New("an array mixes types")
			}
		}
		if t == 0 {
			t = ua.TypeIDString
		}
		return variantOfType(t, x, nil)
	default:
		t, err := typeOf(x)
		if err != nil {
			return nil, err
		}
		return variantOfType(t, x, nil)
	}
}

func numeric(t ua.TypeID) bool {
	return t == ua.TypeIDInt64 || t == ua.TypeIDUint64 || t == ua.TypeIDDouble
}

// variantOf reads a reversible Variant's type, body and dimensions.
func variantOf(typ, body, dims any) (*ua.Variant, error) {
	n, err := uintOf(typ, 8)
	if err != nil {
		return nil, fmt.Errorf("the variant type: %w", err)
	}
	if n == 0 {
		return ua.MustVariant(nil), nil
	}
	if n > uint64(ua.TypeIDDiagnosticInfo) {
		return nil, fmt.Errorf("unexpected variant type: %d", n)
	}
	var shape []int
	if list, ok := dims.([]any); ok {
		for _, d := range list {
			v, err := uintOf(d, 31)
			if err != nil {
				return nil, fmt.Errorf("member Dimensions: %w", err)
			}
			shape = append(shape, int(v))
		}
	}
	return variantOfType(ua.TypeID(n), body, shape)
}

// variantOfType builds a Variant of type t from a parsed body: a scalar, an
// array, or null, which is the type's default value.
func variantOfType(t ua.TypeID, body any, dims []int) (*ua.Variant, error) {
	if list, ok := body.([]any); ok {
		elem := elemType(t)
		flat := reflect.MakeSlice(reflect.SliceOf(elem), len(list), len(list))
		for i, item := range list {
			v, err := scalarOf(t, item)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			flat.Index(i).Set(reflect.ValueOf(v))
		}
		value := flat
		if t == ua.TypeIDByte {
			value = flat.Convert(reflect.TypeOf(ua.ByteArray{}))
		}
		if len(dims) > 1 {
			total := 1
			for _, d := range dims {
				total *= d
			}
			if total != len(list) {
				return nil, fmt.Errorf("the Dimensions %v do not hold %d elements", dims, len(list))
			}
			value = reshape(flat, dims)
		}
		return ua.NewVariant(value.Interface())
	}
	v, err := scalarOf(t, body)
	if err != nil {
		return nil, err
	}
	return ua.NewVariant(v)
}

// reshape nests a flat slice into dims, row by row.
func reshape(flat reflect.Value, dims []int) reflect.Value {
	if len(dims) == 1 {
		return flat
	}
	step := flat.Len() / dims[0]
	first := reshape(flat.Slice(0, step), dims[1:])
	out := reflect.MakeSlice(reflect.SliceOf(first.Type()), dims[0], dims[0])
	for i := 0; i < dims[0]; i++ {
		out.Index(i).Set(reshape(flat.Slice(i*step, (i+1)*step), dims[1:]))
	}
	return out
}

// elemType is the Go type gopcua holds a built-in type's value in.
func elemType(t ua.TypeID) reflect.Type {
	switch t {
	case ua.TypeIDBoolean:
		return reflect.TypeOf(false)
	case ua.TypeIDSByte:
		return reflect.TypeOf(int8(0))
	case ua.TypeIDByte:
		return reflect.TypeOf(uint8(0))
	case ua.TypeIDInt16:
		return reflect.TypeOf(int16(0))
	case ua.TypeIDUint16:
		return reflect.TypeOf(uint16(0))
	case ua.TypeIDInt32:
		return reflect.TypeOf(int32(0))
	case ua.TypeIDUint32:
		return reflect.TypeOf(uint32(0))
	case ua.TypeIDInt64:
		return reflect.TypeOf(int64(0))
	case ua.TypeIDUint64:
		return reflect.TypeOf(uint64(0))
	case ua.TypeIDFloat:
		return reflect.TypeOf(float32(0))
	case ua.TypeIDDouble:
		return reflect.TypeOf(float64(0))
	case ua.TypeIDString:
		return reflect.TypeOf("")
	case ua.TypeIDDateTime:
		return reflect.TypeOf(time.Time{})
	case ua.TypeIDGUID:
		return reflect.TypeOf(new(ua.GUID))
	case ua.TypeIDByteString:
		return reflect.TypeOf([]byte(nil))
	case ua.TypeIDXMLElement:
		return reflect.TypeOf(ua.XMLElement(""))
	case ua.TypeIDNodeID:
		return reflect.TypeOf(new(ua.NodeID))
	case ua.TypeIDExpandedNodeID:
		return reflect.TypeOf(new(ua.ExpandedNodeID))
	case ua.TypeIDStatusCode:
		return reflect.TypeOf(ua.StatusCode(0))
	case ua.TypeIDQualifiedName:
		return reflect.TypeOf(new(ua.QualifiedName))
	case ua.TypeIDLocalizedText:
		return reflect.TypeOf(new(ua.LocalizedText))
	case ua.TypeIDDataValue:
		return reflect.TypeOf(new(ua.DataValue))
	case ua.TypeIDVariant:
		return reflect.TypeOf(new(ua.Variant))
	}
	return reflect.TypeOf((*any)(nil)).Elem()
}

// scalarOf reads one value of built-in type t; null is the type's default.
func scalarOf(t ua.TypeID, v any) (any, error) {
	switch t {
	case ua.TypeIDBoolean:
		b, _ := v.(bool)
		if v != nil && !isBool(v) {
			return nil, fmt.Errorf("a Boolean is true or false, not %v", v)
		}
		return b, nil
	case ua.TypeIDSByte:
		n, err := intOf(v, 8)
		return int8(n), err
	case ua.TypeIDByte:
		n, err := uintOf(v, 8)
		return uint8(n), err
	case ua.TypeIDInt16:
		n, err := intOf(v, 16)
		return int16(n), err
	case ua.TypeIDUint16:
		n, err := uintOf(v, 16)
		return uint16(n), err
	case ua.TypeIDInt32:
		n, err := intOf(v, 32)
		return int32(n), err
	case ua.TypeIDUint32:
		n, err := uintOf(v, 32)
		return uint32(n), err
	case ua.TypeIDInt64:
		return intOf(v, 64)
	case ua.TypeIDUint64:
		return uintOf(v, 64)
	case ua.TypeIDFloat:
		f, err := floatOf(v, 32)
		return float32(f), err
	case ua.TypeIDDouble:
		return floatOf(v, 64)
	case ua.TypeIDString:
		s, err := stringOf(v)
		return s, err
	case ua.TypeIDXMLElement:
		s, err := stringOf(v)
		return ua.XMLElement(s), err
	case ua.TypeIDDateTime:
		s, err := stringOf(v)
		if err != nil || s == "" {
			return time.Time{}, err
		}
		return parseTime(s)
	case ua.TypeIDGUID:
		s, err := stringOf(v)
		if err != nil {
			return nil, err
		}
		if s == "" {
			return ua.NewGUID("00000000-0000-0000-0000-000000000000"), nil
		}
		g := ua.NewGUID(s)
		if g == nil || len(strings.ReplaceAll(s, "-", "")) != 32 {
			return nil, fmt.Errorf("%q is not a Guid", s)
		}
		return g, nil
	case ua.TypeIDByteString:
		if v == nil {
			return []byte(nil), nil
		}
		s, err := stringOf(v)
		if err != nil {
			return nil, err
		}
		b, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(s), ""))
		if err != nil {
			return nil, fmt.Errorf("cannot decode a base64 ByteString: %w", err)
		}
		return b, nil
	case ua.TypeIDStatusCode:
		n, err := uintOf(v, 32)
		return ua.StatusCode(n), err
	case ua.TypeIDQualifiedName:
		s, err := stringOf(v)
		if err != nil {
			return nil, err
		}
		return qualifiedName(s), nil
	case ua.TypeIDLocalizedText:
		return localizedText(v)
	case ua.TypeIDNodeID:
		return nodeIDOf(v)
	case ua.TypeIDExpandedNodeID:
		m, _ := v.(map[string]any)
		n, err := nodeIDOf(v)
		if err != nil {
			return nil, err
		}
		e := &ua.ExpandedNodeID{NodeID: n}
		if uri, ok := m["Namespace"].(string); ok {
			e.NamespaceURI = uri
		}
		if s, ok := m["ServerUri"]; ok && s != nil {
			idx, err := uintOf(s, 32)
			if err != nil {
				return nil, fmt.Errorf("member ServerUri: %w", err)
			}
			e.ServerIndex = uint32(idx)
		}
		return e, nil
	case ua.TypeIDDataValue:
		if v == nil {
			return &ua.DataValue{}, nil
		}
		return dataValueOf(v)
	case ua.TypeIDVariant:
		m, ok := v.(map[string]any)
		if !ok {
			return infer(v)
		}
		return variantOf(m["Type"], m["Body"], m["Dimensions"])
	}
	return nil, fmt.Errorf("%v values are not read from JSON here", t)
}

func isBool(v any) bool {
	_, ok := v.(bool)
	return ok
}

// numberText is a number given as JSON number or as string (Part 6 writes
// 64-bit ones as strings); null is zero.
func numberText(v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "0", nil
	case json.Number:
		return string(x), nil
	case string:
		return x, nil
	}
	return "", fmt.Errorf("%v is not a number", v)
}

func intOf(v any, bits int) (int64, error) {
	s, err := numberText(v)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(s, 10, bits)
}

func uintOf(v any, bits int) (uint64, error) {
	s, err := numberText(v)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(s, 10, bits)
}

func floatOf(v any, bits int) (float64, error) {
	s, err := numberText(v)
	if err != nil {
		return 0, err
	}
	switch s {
	case "NaN":
		return math.NaN(), nil
	case "Infinity":
		return math.Inf(1), nil
	case "-Infinity":
		return math.Inf(-1), nil
	}
	return strconv.ParseFloat(s, bits)
}

func stringOf(v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "", nil
	case string:
		return x, nil
	}
	return "", fmt.Errorf("%v is not a string", v)
}

// qualifiedName reads "<namespace index>:<name>", or a bare name in
// namespace 0.
func qualifiedName(s string) *ua.QualifiedName {
	if i := strings.IndexByte(s, ':'); i > 0 {
		if ns, err := strconv.ParseUint(s[:i], 10, 16); err == nil {
			return &ua.QualifiedName{NamespaceIndex: uint16(ns), Name: s[i+1:]}
		}
	}
	return &ua.QualifiedName{Name: s}
}

// localizedText reads {"Locale":...,"Text":...}, or a bare string, which a
// non-reversible encoder writes for the text alone.
func localizedText(v any) (*ua.LocalizedText, error) {
	switch x := v.(type) {
	case nil:
		return &ua.LocalizedText{}, nil
	case string:
		return ua.NewLocalizedText(x), nil
	case map[string]any:
		locale, err := stringOf(x["Locale"])
		if err != nil {
			return nil, fmt.Errorf("member Locale: %w", err)
		}
		text, err := stringOf(x["Text"])
		if err != nil {
			return nil, fmt.Errorf("member Text: %w", err)
		}
		return ua.NewLocalizedTextWithLocale(text, locale), nil
	}
	return nil, fmt.Errorf("%v is not a LocalizedText", v)
}

// nodeIDOf reads a NodeId object: IdType 0 (or none) numeric, 1 string,
// 2 Guid, 3 ByteString, and a numeric Namespace.
func nodeIDOf(v any) (*ua.NodeID, error) {
	if v == nil {
		return numericNodeID(0, 0), nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%v is not a NodeId", v)
	}
	var ns uint64
	if n, ok := m["Namespace"].(json.Number); ok {
		var err error
		if ns, err = strconv.ParseUint(string(n), 10, 16); err != nil {
			return nil, fmt.Errorf("member Namespace: %w", err)
		}
	}
	kind, err := uintOf(m["IdType"], 8)
	if err != nil {
		return nil, fmt.Errorf("member IdType: %w", err)
	}
	switch kind {
	case 0:
		id, err := uintOf(m["Id"], 32)
		if err != nil {
			return nil, fmt.Errorf("member Id: %w", err)
		}
		return numericNodeID(uint16(ns), uint32(id)), nil
	case 1, 2, 3:
		s, err := stringOf(m["Id"])
		if err != nil || s == "" {
			return nil, errors.New("invalid NodeId, empty identifier")
		}
		switch kind {
		case 1:
			return ua.NewStringNodeID(uint16(ns), s), nil
		case 2:
			if ua.NewGUID(s) == nil {
				return nil, fmt.Errorf("%q is not a Guid", s)
			}
			return ua.NewGUIDNodeID(uint16(ns), s), nil
		default:
			b, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return nil, fmt.Errorf("cannot decode a ByteString identifier: %w", err)
			}
			return ua.NewByteStringNodeID(uint16(ns), b), nil
		}
	}
	return nil, fmt.Errorf("unexpected NodeId IdType %d", kind)
}

// numericNodeID is a numeric NodeId in the most compact of its binary forms,
// as other OPC UA stacks write one: two bytes for a small id in namespace 0,
// four for one that fits in 16 bits in a namespace below 256.
func numericNodeID(ns uint16, id uint32) *ua.NodeID {
	switch {
	case ns == 0 && id < 256:
		return ua.NewTwoByteNodeID(uint8(id))
	case ns < 256 && id < 65536:
		return ua.NewFourByteNodeID(uint8(ns), uint16(id))
	}
	return ua.NewNumericNodeID(ns, id)
}
