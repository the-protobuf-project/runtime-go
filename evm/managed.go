package evm

// managed.go fills driver-managed columns (a generated key, audit timestamps)
// the proto message does not carry, before a record is ABI-encoded for a write —
// the chain counterpart of the orm driver's fillManaged.

import (
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"

	"github.com/the-protobuf-project/runtime-go/store"
)

// fillManaged supplies generated keys and audit timestamps. onCreate generates
// keys and sets both create and update timestamps; otherwise only AutoUpdate
// columns are touched.
func fillManaged(res *store.Resource, cols map[string]any, onCreate bool) {
	now := time.Now().UTC()
	for _, c := range res.Columns {
		switch {
		case onCreate && c.Generated != "" && isEmpty(cols[c.Name]):
			cols[c.Name] = generateID(c.Generated)
		case onCreate && (c.AutoCreate || c.AutoUpdate):
			cols[c.Name] = now
		case !onCreate && c.AutoUpdate:
			cols[c.Name] = now
		}
	}
}

func generateID(strategy string) string {
	switch strategy {
	case "uuid":
		return uuid.NewString()
	default: // "ulid"
		return ulid.MustNew(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano()))).String()
	}
}

func isEmpty(v any) bool {
	if v == nil {
		return true
	}
	s, ok := v.(string)
	return ok && s == ""
}
