package cache

import "context"

// Indexed is [Document] plus lookups by a field other than the id.
//
// Caches are usually read by a key the caller does not have. It has the e-mail
// address and wants the user; it has the order number and wants the cart.
// Without this, that means either a second cache mapping one to the other —
// hand-rolled, and wrong the first time a value is updated — or a full scan.
//
// The index is maintained on write and costs one more write per indexed field.
// Entries expire on their own and leave their index members behind, so lookups
// verify liveness and sweep as they go, exactly as [Document.Keys] does.
//
// A backend with no sets cannot offer any of this and reports [ErrUnsupported].
//
// It inherits [Document]'s scaling limit and adds to it: every indexed field is
// another set, another single key, and another node that every write to that
// field must reach. An index on something low-cardinality — a tenant, a status,
// a version — puts every entry sharing that value into one key, which is the
// case where this stops scaling first and does it quietly.
type Indexed interface {
	Document

	// ByIndex decodes every live entry filed under field=value into dest, which
	// must be a non-nil pointer to a slice. No match is an empty slice, not
	// [ErrNotFound]: asking which entries match is a different question from
	// asking for one that should be there.
	ByIndex(ctx context.Context, field, value string, dest any) error

	// IDsByIndex returns the ids filed under field=value, for when the ids are
	// all you need and decoding every value would be waste.
	IDsByIndex(ctx context.Context, field, value string) ([]string, error)

	// DeleteByIndex removes every entry filed under field=value and returns how
	// many went. This is cache invalidation by group — every entry for one
	// tenant, one user, one version of a computation — and it is what makes an
	// index worth maintaining even for a cache nobody queries by field.
	DeleteByIndex(ctx context.Context, field, value string) (int, error)
}
