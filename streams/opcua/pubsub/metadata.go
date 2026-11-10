package pubsub

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/gopcua/opcua/ua"
)

// DataSetMetaData in Part 6 JSON, member for member as async-opcua writes
// the structure: in declaration order, each left out when it is OPC UA's
// null (a zero number, an empty string, a nil list, a null NodeId or Guid),
// and the structures it nests (Description, ConfigurationVersion) always
// written, as {} when empty.
//
// The data type descriptions (StructureDataTypes, EnumDataTypes,
// SimpleDataTypes) describe a server's own structures. gopcua does not
// encode them as JSON, so an empty list is written as [] and a non-empty one
// is refused; reading skips them.

// putMetaData writes a DataSetMetaDataType.
func putMetaData(w *jsonWriter, m *ua.DataSetMetaDataType) error {
	w.beginObject()
	if m.Namespaces != nil {
		w.name("Namespaces")
		w.beginArray()
		for _, ns := range m.Namespaces {
			w.str(ns)
		}
		w.endArray()
	}
	for _, list := range []struct {
		name string
		n    int
		nil  bool
	}{
		{"StructureDataTypes", len(m.StructureDataTypes), m.StructureDataTypes == nil},
		{"EnumDataTypes", len(m.EnumDataTypes), m.EnumDataTypes == nil},
		{"SimpleDataTypes", len(m.SimpleDataTypes), m.SimpleDataTypes == nil},
	} {
		if list.n > 0 {
			return fmt.Errorf("%s cannot be written as JSON here; describe the fields with built-in types", list.name)
		}
		if !list.nil {
			w.name(list.name)
			w.beginArray()
			w.endArray()
		}
	}
	putStr(w, "Name", m.Name)
	w.name("Description")
	putLocalized(w, m.Description)
	if m.Fields != nil {
		w.name("Fields")
		w.beginArray()
		for _, f := range m.Fields {
			if err := putFieldMetaData(w, f); err != nil {
				return err
			}
		}
		w.endArray()
	}
	putGUID(w, "DataSetClassId", m.DataSetClassID)
	w.name("ConfigurationVersion")
	putVersion(w, m.ConfigurationVersion)
	w.endObject()
	return nil
}

// putFieldMetaData writes one FieldMetaData.
func putFieldMetaData(w *jsonWriter, f *ua.FieldMetaData) error {
	if f == nil {
		f = &ua.FieldMetaData{}
	}
	w.beginObject()
	putStr(w, "Name", f.Name)
	w.name("Description")
	putLocalized(w, f.Description)
	if f.FieldFlags != 0 {
		w.name("FieldFlags")
		w.int(int64(f.FieldFlags))
	}
	if f.BuiltInType != 0 {
		w.name("BuiltInType")
		w.uint(uint64(f.BuiltInType))
	}
	if !nullNodeID(f.DataType) {
		w.name("DataType")
		putNodeID(w, f.DataType, "", 0)
	}
	if f.ValueRank != 0 {
		w.name("ValueRank")
		w.int(int64(f.ValueRank))
	}
	if f.ArrayDimensions != nil {
		w.name("ArrayDimensions")
		w.beginArray()
		for _, d := range f.ArrayDimensions {
			w.uint(uint64(d))
		}
		w.endArray()
	}
	if f.MaxStringLength != 0 {
		w.name("MaxStringLength")
		w.uint(uint64(f.MaxStringLength))
	}
	putGUID(w, "DataSetFieldId", f.DataSetFieldID)
	if f.Properties != nil {
		w.name("Properties")
		w.beginArray()
		for _, p := range f.Properties {
			w.beginObject()
			if p != nil {
				if p.Key != nil && (p.Key.Name != "" || p.Key.NamespaceIndex != 0) {
					w.name("Key")
					if err := putBody(w, ua.TypeIDQualifiedName, p.Key); err != nil {
						return err
					}
				}
				if p.Value != nil && p.Value.Value() != nil {
					w.name("Value")
					if err := putVariant(w, p.Value); err != nil {
						return err
					}
				}
			}
			w.endObject()
		}
		w.endArray()
	}
	w.endObject()
	return nil
}

// putLocalized writes a LocalizedText, {} when it is empty.
func putLocalized(w *jsonWriter, l *ua.LocalizedText) {
	_ = putBody(w, ua.TypeIDLocalizedText, l)
}

// putGUID writes a Guid unless it is nil or all zeros.
func putGUID(w *jsonWriter, name string, g *ua.GUID) {
	if g == nil {
		return
	}
	s := strings.ToLower(g.String())
	if s == "00000000-0000-0000-0000-000000000000" {
		return
	}
	w.name(name)
	w.str(s)
}

// nullNodeID reports whether n is OPC UA's null NodeId, i=0 in namespace 0.
func nullNodeID(n *ua.NodeID) bool {
	return n == nil || n.Namespace() == 0 && n.Type() <= ua.NodeIDTypeNumeric && n.IntID() == 0
}

// metaDataOf reads a DataSetMetaDataType. A LocalizedText may be its text
// alone, as non-reversible encoders write it.
func metaDataOf(raw json.RawMessage) (*ua.DataSetMetaDataType, error) {
	v, err := parseJSON(raw)
	if err != nil {
		return nil, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, errors.New("MetaData is a JSON object")
	}
	out := &ua.DataSetMetaDataType{}
	if list, ok := m["Namespaces"].([]any); ok {
		out.Namespaces = make([]string, 0, len(list))
		for _, ns := range list {
			s, serr := stringOf(ns)
			if serr != nil {
				return nil, fmt.Errorf("member Namespaces: %w", err)
			}
			out.Namespaces = append(out.Namespaces, s)
		}
	}
	if out.Name, err = stringOf(m["Name"]); err != nil {
		return nil, fmt.Errorf("member Name: %w", err)
	}
	if out.Description, err = optionalLocalized(m["Description"]); err != nil {
		return nil, fmt.Errorf("member Description: %w", err)
	}
	if list, ok := m["Fields"].([]any); ok {
		out.Fields = make([]*ua.FieldMetaData, 0, len(list))
		for i, item := range list {
			f, ferr := fieldMetaDataOf(item)
			if ferr != nil {
				return nil, fmt.Errorf("member Fields[%d]: %w", i, ferr)
			}
			out.Fields = append(out.Fields, f)
		}
	}
	if out.DataSetClassID, err = guidOf(m["DataSetClassId"]); err != nil {
		return nil, fmt.Errorf("member DataSetClassId: %w", err)
	}
	if c, ok := m["ConfigurationVersion"]; ok && c != nil {
		b, _ := json.Marshal(c)
		if err = versionOf(b, &out.ConfigurationVersion); err != nil {
			return nil, fmt.Errorf("member ConfigurationVersion: %w", err)
		}
	}
	if out.ConfigurationVersion == nil {
		out.ConfigurationVersion = &ua.ConfigurationVersionDataType{}
	}
	return out, nil
}

// fieldMetaDataOf reads one FieldMetaData.
func fieldMetaDataOf(v any) (*ua.FieldMetaData, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, errors.New("a FieldMetaData is a JSON object")
	}
	f := &ua.FieldMetaData{}
	var err error
	if f.Name, err = stringOf(m["Name"]); err != nil {
		return nil, fmt.Errorf("member Name: %w", err)
	}
	if f.Description, err = optionalLocalized(m["Description"]); err != nil {
		return nil, fmt.Errorf("member Description: %w", err)
	}
	flags, err := intOf(m["FieldFlags"], 16)
	if err != nil {
		return nil, fmt.Errorf("member FieldFlags: %w", err)
	}
	f.FieldFlags = ua.DataSetFieldFlags(flags)
	builtIn, err := uintOf(m["BuiltInType"], 8)
	if err != nil {
		return nil, fmt.Errorf("member BuiltInType: %w", err)
	}
	f.BuiltInType = uint8(builtIn)
	if d, ok := m["DataType"]; ok && d != nil {
		if f.DataType, err = nodeIDOf(d); err != nil {
			return nil, fmt.Errorf("member DataType: %w", err)
		}
	}
	rank, err := intOf(m["ValueRank"], 32)
	if err != nil {
		return nil, fmt.Errorf("member ValueRank: %w", err)
	}
	f.ValueRank = int32(rank)
	if list, ok := m["ArrayDimensions"].([]any); ok {
		f.ArrayDimensions = make([]uint32, 0, len(list))
		for _, d := range list {
			n, derr := uintOf(d, 32)
			if derr != nil {
				return nil, fmt.Errorf("member ArrayDimensions: %w", derr)
			}
			f.ArrayDimensions = append(f.ArrayDimensions, uint32(n))
		}
	}
	max, err := uintOf(m["MaxStringLength"], 32)
	if err != nil {
		return nil, fmt.Errorf("member MaxStringLength: %w", err)
	}
	f.MaxStringLength = uint32(max)
	if f.DataSetFieldID, err = guidOf(m["DataSetFieldId"]); err != nil {
		return nil, fmt.Errorf("member DataSetFieldId: %w", err)
	}
	if list, ok := m["Properties"].([]any); ok {
		f.Properties = make([]*ua.KeyValuePair, 0, len(list))
		for _, item := range list {
			p, _ := item.(map[string]any)
			kv := &ua.KeyValuePair{}
			if k, kerr := stringOf(p["Key"]); kerr == nil && k != "" {
				kv.Key = qualifiedName(k)
			}
			if val, ok := p["Value"].(map[string]any); ok {
				if kv.Value, err = variantOf(val["Type"], val["Body"], val["Dimensions"]); err != nil {
					return nil, fmt.Errorf("member Properties: %w", err)
				}
			}
			f.Properties = append(f.Properties, kv)
		}
	}
	return f, nil
}

// optionalLocalized reads a LocalizedText, nil when absent or empty, as
// gopcua holds one nobody set.
func optionalLocalized(v any) (*ua.LocalizedText, error) {
	l, err := localizedText(v)
	if err != nil {
		return nil, err
	}
	if l.Locale == "" && l.Text == "" {
		l = nil
	}
	return l, err
}

// guidOf reads a Guid string, nil when absent.
func guidOf(v any) (*ua.GUID, error) {
	s, err := stringOf(v)
	if err != nil || s == "" {
		return nil, err
	}
	g := ua.NewGUID(s)
	if g == nil || len(strings.ReplaceAll(s, "-", "")) != 32 {
		return nil, fmt.Errorf("%q is not a Guid", s)
	}
	return g, nil
}
