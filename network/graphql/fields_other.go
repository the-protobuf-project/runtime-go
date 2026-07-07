package graphql

import "encoding/json"

// BoolField is a generated handle for a boolean column.
type BoolField struct{ Col string }

// Eq matches rows where the column equals v.
func (f BoolField) Eq(v bool) Predicate { return pred(f.Col, "_eq", v) }

// Neq matches rows where the column does not equal v.
func (f BoolField) Neq(v bool) Predicate { return pred(f.Col, "_neq", v) }

// IsNull matches rows where the column is (v=true) or is not (v=false) null.
func (f BoolField) IsNull(v bool) Predicate { return pred(f.Col, "_is_null", v) }

// Asc orders results by this column ascending.
func (f BoolField) Asc() OrderTerm { return OrderTerm{f.Col, Asc} }

// Desc orders results by this column descending.
func (f BoolField) Desc() OrderTerm { return OrderTerm{f.Col, Desc} }

// JSONField is a generated handle for a JSON/JSONB column. Filtering is limited to
// equality and null checks.
type JSONField struct{ Col string }

// Eq matches rows where the column equals the JSON value v.
func (f JSONField) Eq(v json.RawMessage) Predicate { return pred(f.Col, "_eq", v) }

// IsNull matches rows where the column is (v=true) or is not (v=false) null.
func (f JSONField) IsNull(v bool) Predicate { return pred(f.Col, "_is_null", v) }

// EnumField is a generated handle for an enum column, parameterized by the enum type so
// operators take typed values.
type EnumField[E comparable] struct{ Col string }

// Eq matches rows where the column equals v.
func (f EnumField[E]) Eq(v E) Predicate { return pred(f.Col, "_eq", v) }

// Neq matches rows where the column does not equal v.
func (f EnumField[E]) Neq(v E) Predicate { return pred(f.Col, "_neq", v) }

// In matches rows where the column is one of vs.
func (f EnumField[E]) In(vs ...E) Predicate { return pred(f.Col, "_in", vs) }

// IsNull matches rows where the column is (v=true) or is not (v=false) null.
func (f EnumField[E]) IsNull(v bool) Predicate { return pred(f.Col, "_is_null", v) }

// Asc orders results by this column ascending.
func (f EnumField[E]) Asc() OrderTerm { return OrderTerm{f.Col, Asc} }

// Desc orders results by this column descending.
func (f EnumField[E]) Desc() OrderTerm { return OrderTerm{f.Col, Desc} }
