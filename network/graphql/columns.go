package graphql

import (
	"encoding/json"
	"reflect"
	"strings"
)

// OrderBy is the sort direction for an order_by term. It is the standard ascending/
// descending enum shared by every GraphQL CRUD resource, so it lives here rather than
// being generated per schema.
type OrderBy string

const (
	Asc  OrderBy = "Asc"
	Desc OrderBy = "Desc"
)

// OrderTerm is one sort key (column + direction) produced by a field handle's Asc/Desc.
// A Find request's OrderBy takes a list of them.
type OrderTerm struct {
	col string
	dir OrderBy
}

// MarshalJSON encodes the term as {col: direction}.
func (o OrderTerm) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]OrderBy{o.col: o.dir})
}

// IsOmitted reports whether v is its type's zero value. Generated operation code uses it
// to decide whether to send an optional argument: an unset native value, an empty
// Predicate, or a nil slice is omitted from the operation entirely.
func IsOmitted(v any) bool {
	rv := reflect.ValueOf(v)
	return !rv.IsValid() || rv.IsZero()
}

// columnValue is implemented by Nullable[T]. SetColumns uses it to decide presence by the
// field's explicit three-state instruction (unset/null/value) rather than by IsZero, so a
// field cleared to null or set to a zero value (e.g. "") is still emitted.
type columnValue interface{ IsSet() bool }

// SetColumns turns an update patch struct into a Hasura-style update-columns map: each
// instructed exported field becomes {jsonName: {"set": value}}. A Nullable[T] field is
// included when IsSet (a value, or an explicit null that marshals to JSON null); any other
// field type is included when non-zero. Field names come from the json struct tag (falling
// back to the Go field name). Generated Update methods pass the result as the update_columns
// variable, so callers write a plain native patch struct.
func SetColumns(patch any) map[string]any {
	out := map[string]any{}
	rv := reflect.ValueOf(patch)
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return out
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return out
	}
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.PkgPath != "" { // skip unexported fields
			continue
		}
		fv := rv.Field(i)
		if cv, ok := fv.Interface().(columnValue); ok {
			if !cv.IsSet() {
				continue
			}
		} else if fv.IsZero() {
			continue
		}
		out[jsonName(f)] = map[string]any{"set": fv.Interface()}
	}
	return out
}

// jsonName returns the wire name of a struct field from its json tag, or the field name.
func jsonName(f reflect.StructField) string {
	tag := f.Tag.Get("json")
	if i := strings.IndexByte(tag, ','); i >= 0 {
		tag = tag[:i]
	}
	if tag == "" {
		return f.Name
	}
	return tag
}
