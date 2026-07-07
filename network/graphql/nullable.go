package graphql

import "encoding/json"

// Nullable is a three-state value for masked update inputs. A column field can be:
//
//   - unset  — the zero Nullable; SetColumns omits it, so the column is left unchanged.
//   - null   — graphql.Null[T](); the column is cleared to SQL NULL (Hasura `_set: {col: null}`).
//   - value  — graphql.Value(v); the column is written to v (including v's zero value).
//
// A plain T field with json ",omitzero" cannot express the difference between "leave unchanged"
// and "clear to null" (both look like the zero value), so a masked Update could never clear an
// optional column — diverging from GORM, which writes NULL. Nullable removes that divergence.
//
//	patch := UpdateInput{
//	    DisplayName: graphql.Value("Bob"),   // set the column to "Bob"
//	    Description: graphql.Null[string](),  // clear the column to NULL
//	    // Code left as its zero Nullable      // omitted: column unchanged
//	}
type Nullable[T any] struct {
	set  bool
	null bool
	val  T
}

// Value returns a Nullable that sets the column to v (v may be its type's zero value, which a
// plain omitzero field could not express).
func Value[T any](v T) Nullable[T] { return Nullable[T]{set: true, val: v} }

// Null returns a Nullable that clears the column to SQL NULL.
func Null[T any]() Nullable[T] { return Nullable[T]{set: true, null: true} }

// Unset returns the zero Nullable, which SetColumns omits so the column is left unchanged. The
// zero value of any Nullable is already unset; Unset exists for symmetry and readable intent.
func Unset[T any]() Nullable[T] { return Nullable[T]{} }

// IsSet reports whether the field carries an instruction (a value or an explicit null). An
// unset field returns false and is omitted from the mutation.
func (n Nullable[T]) IsSet() bool { return n.set }

// IsNull reports whether the field clears the column to NULL.
func (n Nullable[T]) IsNull() bool { return n.set && n.null }

// Get returns the held value and whether a concrete value is present (false when unset or null).
func (n Nullable[T]) Get() (T, bool) { return n.val, n.set && !n.null }

// MarshalJSON emits JSON null for an explicit null, otherwise the encoded value. SetColumns
// drops unset fields before marshaling, so an unset Nullable never reaches the wire.
func (n Nullable[T]) MarshalJSON() ([]byte, error) {
	if n.null {
		return []byte("null"), nil
	}
	return json.Marshal(n.val)
}
