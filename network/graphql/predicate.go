package graphql

import "encoding/json"

// Predicate is a filter expression for a resource's Find/where argument. It is built from
// the generated per-resource field handles (e.g. resource.Id.Eq("x")) and combined with
// And/Or/Not. The zero Predicate is empty; generated code omits it from the operation
// entirely rather than sending an empty object.
type Predicate struct {
	node map[string]any
}

// MarshalJSON encodes the underlying filter node (a GraphQL BoolExp object).
func (p Predicate) MarshalJSON() ([]byte, error) {
	if p.node == nil {
		return []byte("null"), nil
	}
	return json.Marshal(p.node)
}

// pred builds a single-column predicate of the form {col: {op: v}}. It is the shared
// constructor used by every field handle's operator method.
func pred(col, op string, v any) Predicate {
	return Predicate{node: map[string]any{col: map[string]any{op: v}}}
}

// And combines predicates so that all must match ({_and: [...]}). With no arguments it
// returns the empty predicate.
func And(ps ...Predicate) Predicate { return combine("_and", ps) }

// Or combines predicates so that any may match ({_or: [...]}).
func Or(ps ...Predicate) Predicate { return combine("_or", ps) }

// Not negates a predicate ({_not: ...}).
func Not(p Predicate) Predicate { return Predicate{node: map[string]any{"_not": p.node}} }

// Relation nests a related resource's predicate under a relationship field, so a row can
// be filtered by its relations, e.g. resource.OrganisationMembers(members.Email.Eq("x")).
func Relation(col string, p Predicate) Predicate {
	return Predicate{node: map[string]any{col: p.node}}
}

func combine(op string, ps []Predicate) Predicate {
	if len(ps) == 0 {
		return Predicate{}
	}
	nodes := make([]any, len(ps))
	for i, p := range ps {
		nodes[i] = p.node
	}
	return Predicate{node: map[string]any{op: nodes}}
}
