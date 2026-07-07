// Package graphql provides small helpers and scalar types for generated GraphQL
// clients: pointer constructors for optional (nullable) arguments, and scalar types
// that tolerate engine-specific JSON encodings.
package graphql

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Ptr returns a pointer to v. It works for any type, including generated enums, input
// structs, and the scalar types below (e.g. Where: graphql.Ptr(types.UsersBoolExp{})).
func Ptr[T any](v T) *T {
	return &v
}

// Variable carries an operation argument together with its exact GraphQL type (e.g.
// "String1!", "[UsersOrderByExp!]"). go-graphql-client otherwise infers the type from
// the Go kind (string -> String), which is wrong for engine-specific scalars; Var lets
// generated code declare the precise type. The value is serialized as itself.
type Variable struct {
	Value   any
	GQLType string
}

// Var wraps a non-null argument value with its GraphQL type (without the outer "!",
// which go-graphql-client appends for non-pointer values).
func Var(value any, gqlType string) Variable {
	return Variable{Value: value, GQLType: gqlType}
}

// VarPtr wraps a nullable argument value. Returning a pointer keeps go-graphql-client
// from appending the non-null "!" to the declared type.
func VarPtr(value any, gqlType string) *Variable {
	return &Variable{Value: value, GQLType: gqlType}
}

// GetGraphQLType reports the declared GraphQL type to go-graphql-client.
func (v Variable) GetGraphQLType() string { return v.GQLType }

// MarshalJSON serializes the wrapped value (the wrapper itself is transparent).
func (v Variable) MarshalJSON() ([]byte, error) { return json.Marshal(v.Value) }

// Int64 is a 64-bit integer GraphQL scalar. Engines commonly serialize 64-bit integers
// as JSON strings to preserve precision, but may return computed values (e.g. aggregate
// counts) as JSON numbers. Int64 decodes from either form and encodes as a string.
type Int64 int64

// UnmarshalJSON accepts a JSON number or a quoted JSON string.
func (v *Int64) UnmarshalJSON(b []byte) error {
	s := string(b)
	if s == "null" {
		return nil
	}
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	if s == "" {
		return nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("Int64: %w", err)
	}
	*v = Int64(n)
	return nil
}

// MarshalJSON encodes as a quoted string so full 64-bit precision survives the wire.
func (v Int64) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(strconv.FormatInt(int64(v), 10))), nil
}

// Bigdecimal is an arbitrary-precision decimal scalar held as its textual form.
// Engines may return it as a JSON string (to preserve precision) or, for computed
// aggregates, as a JSON number; Bigdecimal decodes either and encodes as a string.
type Bigdecimal string

// UnmarshalJSON accepts a JSON number or a quoted JSON string.
func (v *Bigdecimal) UnmarshalJSON(b []byte) error {
	s := string(b)
	if s == "null" {
		return nil
	}
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	*v = Bigdecimal(s)
	return nil
}

// MarshalJSON encodes as a quoted string to preserve precision on the wire.
func (v Bigdecimal) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(string(v))), nil
}

// Typed scalar pointer constructors for optional arguments.

// String returns a pointer to a string value (GraphQL String/String1/ID/Bigdecimal/
// Timestamp/Timestamptz, which all map to string).
func String(v string) *string { return &v }

// Bool returns a pointer to a bool value (GraphQL Boolean/Boolean1).
func Bool(v bool) *bool { return &v }

// Int returns a pointer to an int value (GraphQL Int).
func Int(v int) *int { return &v }

// Int32 returns a pointer to an int32 value (GraphQL Int32).
func Int32(v int32) *int32 { return &v }

// Float64 returns a pointer to a float64 value (GraphQL Float/Float64).
func Float64(v float64) *float64 { return &v }

// JSON returns a pointer to a json.RawMessage value (GraphQL Json).
func JSON(v json.RawMessage) *json.RawMessage { return &v }
