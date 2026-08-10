package core

import (
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// FillManaged supplies the values a driver provides rather than the message: a
// generated primary key and the audit timestamps.
//
// onCreate distinguishes the two cases. A create generates the key it was not
// given and stamps both timestamps; an update touches only the columns marked
// AutoUpdate, because a created_at that moves is not an audit trail.
//
// It works on the column map rather than the message so the same code serves a
// backend writing columns and one writing a document — and so a driver can pass
// the filled map straight to [store.ColumnsToMessage] and hand the caller back
// what was actually stored rather than what it was given.
func FillManaged(res *store.Resource, cols map[string]any, onCreate bool) {
	now := time.Now().UTC()
	for _, c := range res.Columns {
		switch {
		case onCreate && c.Generated != "" && IsEmpty(cols[c.Name]):
			cols[c.Name] = GenerateID(c.Generated)
		case onCreate && (c.AutoCreate || c.AutoUpdate):
			cols[c.Name] = now
		case !onCreate && c.AutoUpdate:
			cols[c.Name] = now
		}
	}
}

// HasManaged reports whether a resource has any column a driver must fill,
// so a backend can skip the round trip through the column map when none does.
func HasManaged(res *store.Resource) bool {
	for _, c := range res.Columns {
		if c.Managed() {
			return true
		}
	}
	return false
}

// GenerateID returns a new identifier for the named strategy.
//
// ULID by default, and by default deliberately: it sorts by creation time, so
// records land in insertion order in any store that keys on it, and a listing
// ordered by id is chronological without a second column to sort on.
func GenerateID(strategy string) string {
	switch strategy {
	case "uuid":
		return uuid.NewString()
	default: // "ulid"
		return ulid.MustNew(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano()))).String()
	}
}

// IsEmpty reports whether a column value counts as unset for the purpose of
// generating one.
func IsEmpty(v any) bool {
	if v == nil {
		return true
	}
	s, ok := v.(string)
	return ok && s == ""
}
