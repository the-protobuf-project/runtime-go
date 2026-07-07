package graphql

// Int64Field is a generated handle for a 64-bit integer column.
type Int64Field struct{ Col string }

// Eq matches rows where the column equals v.
func (f Int64Field) Eq(v Int64) Predicate { return pred(f.Col, "_eq", v) }

// Neq matches rows where the column does not equal v.
func (f Int64Field) Neq(v Int64) Predicate { return pred(f.Col, "_neq", v) }

// Gt matches rows where the column is greater than v.
func (f Int64Field) Gt(v Int64) Predicate { return pred(f.Col, "_gt", v) }

// Gte matches rows where the column is greater than or equal to v.
func (f Int64Field) Gte(v Int64) Predicate { return pred(f.Col, "_gte", v) }

// Lt matches rows where the column is less than v.
func (f Int64Field) Lt(v Int64) Predicate { return pred(f.Col, "_lt", v) }

// Lte matches rows where the column is less than or equal to v.
func (f Int64Field) Lte(v Int64) Predicate { return pred(f.Col, "_lte", v) }

// In matches rows where the column is one of vs.
func (f Int64Field) In(vs ...Int64) Predicate { return pred(f.Col, "_in", vs) }

// IsNull matches rows where the column is (v=true) or is not (v=false) null.
func (f Int64Field) IsNull(v bool) Predicate { return pred(f.Col, "_is_null", v) }

// Asc orders results by this column ascending.
func (f Int64Field) Asc() OrderTerm { return OrderTerm{f.Col, Asc} }

// Desc orders results by this column descending.
func (f Int64Field) Desc() OrderTerm { return OrderTerm{f.Col, Desc} }

// FloatField is a generated handle for a floating-point column.
type FloatField struct{ Col string }

// Eq matches rows where the column equals v.
func (f FloatField) Eq(v float64) Predicate { return pred(f.Col, "_eq", v) }

// Neq matches rows where the column does not equal v.
func (f FloatField) Neq(v float64) Predicate { return pred(f.Col, "_neq", v) }

// Gt matches rows where the column is greater than v.
func (f FloatField) Gt(v float64) Predicate { return pred(f.Col, "_gt", v) }

// Gte matches rows where the column is greater than or equal to v.
func (f FloatField) Gte(v float64) Predicate { return pred(f.Col, "_gte", v) }

// Lt matches rows where the column is less than v.
func (f FloatField) Lt(v float64) Predicate { return pred(f.Col, "_lt", v) }

// Lte matches rows where the column is less than or equal to v.
func (f FloatField) Lte(v float64) Predicate { return pred(f.Col, "_lte", v) }

// In matches rows where the column is one of vs.
func (f FloatField) In(vs ...float64) Predicate { return pred(f.Col, "_in", vs) }

// IsNull matches rows where the column is (v=true) or is not (v=false) null.
func (f FloatField) IsNull(v bool) Predicate { return pred(f.Col, "_is_null", v) }

// Asc orders results by this column ascending.
func (f FloatField) Asc() OrderTerm { return OrderTerm{f.Col, Asc} }

// Desc orders results by this column descending.
func (f FloatField) Desc() OrderTerm { return OrderTerm{f.Col, Desc} }
