// Package ulid generates sortable, collision-resistant identifiers for
// runtime-go modules. It wraps github.com/oklog/ulid/v2 behind the small
// Generate/GetTimeCode/GetRandomCode surface the cache and streams modules
// were already written against, so call sites read the same as before.
//
// Two flavors of code are exposed because the callers want different things
// from an ID:
//
//   - [ID.GetTimeCode] is for identifiers that should sort by creation order —
//     stream keys and published message IDs, where lexicographic order is
//     chronological order.
//   - [ID.GetRandomCode] is for opaque identifiers — document and cache keys,
//     where the key should not advertise when it was written.
//
// Both are unique: the time code is a full ULID (timestamp prefix plus
// randomness), not the bare timestamp, so two IDs minted in the same
// millisecond do not collide.
package ulid
