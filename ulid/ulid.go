package ulid

import "github.com/oklog/ulid/v2"

// ID is a generated identifier. Use [Generate] to mint one, then take the
// representation the call site needs.
type ID struct {
	ulid ulid.ULID
}

// Generate mints a new identifier from the current time plus entropy. It is
// safe for concurrent use, and IDs minted within the same millisecond are
// monotonically increasing rather than colliding.
func Generate() ID {
	return ID{ulid: ulid.Make()}
}

// GetTimeCode returns the full 26-character ULID: a Crockford base32 timestamp
// prefix followed by randomness. Sorting these strings sorts by creation time,
// which is what the stream and publisher paths rely on.
//
// The timestamp alone is deliberately not returned — it repeats for everything
// minted in the same millisecond, and these values are used as keys.
func (i ID) GetTimeCode() string {
	return i.ulid.String()
}

// GetRandomCode returns the 16-character random component of the ULID, with the
// timestamp prefix stripped. It carries 80 bits of entropy and reveals nothing
// about when it was generated, which is what the document and cache key paths
// want.
func (i ID) GetRandomCode() string {
	// A ULID string is always 26 characters: 10 of encoded timestamp followed
	// by 16 of encoded entropy.
	return i.ulid.String()[10:]
}

// String returns the full ULID, matching [ID.GetTimeCode].
func (i ID) String() string {
	return i.ulid.String()
}
