package graphql

// StringField is a generated handle for a string-like column (text, id, timestamp). Its
// methods build a Predicate for the column; ordered comparisons treat the value
// lexically, which is correct for ISO-8601 timestamps too.
type StringField struct{ Col string }

// Eq matches rows where the column equals v.
func (f StringField) Eq(v string) Predicate { return pred(f.Col, "_eq", v) }

// Neq matches rows where the column does not equal v.
func (f StringField) Neq(v string) Predicate { return pred(f.Col, "_neq", v) }

// Gt matches rows where the column is greater than v.
func (f StringField) Gt(v string) Predicate { return pred(f.Col, "_gt", v) }

// Gte matches rows where the column is greater than or equal to v.
func (f StringField) Gte(v string) Predicate { return pred(f.Col, "_gte", v) }

// Lt matches rows where the column is less than v.
func (f StringField) Lt(v string) Predicate { return pred(f.Col, "_lt", v) }

// Lte matches rows where the column is less than or equal to v.
func (f StringField) Lte(v string) Predicate { return pred(f.Col, "_lte", v) }

// In matches rows where the column is one of vs.
func (f StringField) In(vs ...string) Predicate { return pred(f.Col, "_in", vs) }

// Like matches rows where the column matches the SQL LIKE pattern v.
func (f StringField) Like(v string) Predicate { return pred(f.Col, "_like", v) }

// ILike matches rows where the column matches the case-insensitive LIKE pattern v.
func (f StringField) ILike(v string) Predicate { return pred(f.Col, "_ilike", v) }

// Regex matches rows where the column matches the regular expression v.
func (f StringField) Regex(v string) Predicate { return pred(f.Col, "_regex", v) }

// IsNull matches rows where the column is (v=true) or is not (v=false) null.
func (f StringField) IsNull(v bool) Predicate { return pred(f.Col, "_is_null", v) }

// Asc orders results by this column ascending.
func (f StringField) Asc() OrderTerm { return OrderTerm{f.Col, Asc} }

// Desc orders results by this column descending.
func (f StringField) Desc() OrderTerm { return OrderTerm{f.Col, Desc} }
