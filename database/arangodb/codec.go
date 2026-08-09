package arangodb

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database"
)

// The system attributes ArangoDB owns. A stored document carries them and a
// record does not, so they are stripped on the way out — a caller's message has
// no field for _rev and would fail to decode one.
const (
	fieldKey  = "_key"
	fieldID   = "_id"
	fieldRev  = "_rev"
	fieldFrom = "_from"
	fieldTo   = "_to"
)

// toDocument turns a column map into the document stored on the server.
//
// The primary key moves to _key and every other column keeps its own name, so a
// document written here is readable from arangosh and by anything else pointed
// at the same collection.
func toDocument(res *database.Resource, cols map[string]any) (map[string]any, error) {
	doc := make(map[string]any, len(cols))
	for name, value := range cols {
		v := value
		if v != nil {
			v = toStored(res, name, v)
		}
		if name == res.PKColumn {
			key, ok := v.(string)
			if !ok || key == "" {
				return nil, fmt.Errorf("arangodb: resource %q has no primary key value", res.Name)
			}
			doc[fieldKey] = escapeKey(key)
			continue
		}
		if name == "" {
			continue
		}
		doc[name] = v
	}
	return doc, nil
}

// fromDocument turns a stored document back into a record.
func fromDocument(res *database.Resource, doc map[string]any) (proto.Message, error) {
	cols := make(map[string]any, len(doc))
	for name, value := range doc {
		switch name {
		case fieldKey:
			key, _ := value.(string)
			cols[res.PKColumn] = unescapeKey(key)
		case fieldID, fieldRev, fieldFrom, fieldTo:
			// ArangoDB's, not the record's.
		default:
			cols[name] = fromStored(value)
		}
	}
	return database.ColumnsToMessage(res, cols)
}

// toStored narrows a bridge value to something the wire encoding carries.
//
// Only two kinds need it. A timestamp becomes RFC 3339 text, which is what
// ArangoDB's date functions read and what makes a stored document legible; and
// bytes become base64, because JSON has no binary and the driver would
// otherwise encode a []byte as an array of numbers that no other reader would
// recognize as a blob.
func toStored(res *database.Resource, column string, value any) any {
	col, ok := res.LookupColumn(column)
	if !ok {
		return value
	}
	switch col.Kind {
	case database.KindTimestamp:
		t, ok := value.(time.Time)
		if !ok {
			return value
		}
		return t.UTC().Format(time.RFC3339Nano)
	case database.KindBytes:
		b, ok := value.([]byte)
		if !ok {
			return value
		}
		return encodeBytes(b)
	default:
		return value
	}
}

// fromStored widens a stored value back to what the bridge expects.
//
// JSON gives every number back as a float64, which loses integers past 2^53 —
// so a whole-number float is handed back as an int64 where it fits, and the
// bridge converts from there. Anything larger is a value this encoding could not
// have carried faithfully in the first place, and is left as it came so the
// bridge reports the mismatch rather than this quietly rounding it.
func fromStored(value any) any {
	switch v := value.(type) {
	case float64:
		if v == float64(int64(v)) && v >= -maxSafeInteger && v <= maxSafeInteger {
			return int64(v)
		}
		return v
	case string:
		if b, ok := decodeBytes(v); ok {
			return b
		}
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t.UTC()
		}
		return v
	default:
		return value
	}
}

// maxSafeInteger is where a float64 stops representing every integer.
const maxSafeInteger = 1 << 53
