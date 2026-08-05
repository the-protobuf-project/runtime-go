package codec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// Encode turns a value into the bytes stored on the server.
//
// Any JSON-serializable value works — a struct, a map, a slice, a scalar — so a
// model gaining a field is not a change here.
func Encode(value any) ([]byte, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("redis: cannot encode value: %w", err)
	}
	return b, nil
}

// Decode unmarshals stored bytes into dest, which must be a non-nil pointer.
func Decode(data []byte, dest any) error {
	if dest == nil {
		return fmt.Errorf("redis: decode destination is nil")
	}
	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("redis: cannot decode stored value: %w", err)
	}
	return nil
}

// Canonicalize returns a value's deterministic encoding and the SHA256 of it.
//
// Hashing the canonical form rather than the caller's value is what makes
// deduplication reliable: two maps with the same pairs in different insertion
// orders produce identical bytes and therefore the same hash. The value is
// round-tripped through a generic first, so the same record written once as a
// struct and once as a map hashes the same.
func Canonicalize(value any) (hash string, canonical []byte, err error) {
	raw, err := Encode(value)
	if err != nil {
		return "", nil, err
	}

	var generic any
	if uerr := json.Unmarshal(raw, &generic); uerr != nil {
		return "", nil, fmt.Errorf("redis: encoded value is not valid JSON: %w", uerr)
	}

	var buf bytes.Buffer
	if werr := writeCanonical(&buf, generic); werr != nil {
		return "", nil, werr
	}

	sum := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(sum[:]), buf.Bytes(), nil
}

// writeCanonical writes v with object keys in sorted order, so the same content
// always produces the same bytes.
func writeCanonical(buf *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		buf.WriteString("null")

	case bool:
		if x {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}

	case float64, string:
		// json.Unmarshal into any yields only these two scalar kinds; the
		// standard encoder gets their formatting and escaping right.
		b, err := json.Marshal(x)
		if err != nil {
			return fmt.Errorf("redis: cannot encode scalar: %w", err)
		}
		buf.Write(b)

	case []any:
		buf.WriteByte('[')
		for i, el := range x {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonical(buf, el); err != nil {
				return err
			}
		}
		buf.WriteByte(']')

	case map[string]any:
		buf.WriteByte('{')
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			kb, err := json.Marshal(k)
			if err != nil {
				return fmt.Errorf("redis: cannot encode object key: %w", err)
			}
			buf.Write(kb)
			buf.WriteByte(':')
			if err := writeCanonical(buf, x[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')

	default:
		return fmt.Errorf("redis: unexpected type %T after JSON round-trip", x)
	}
	return nil
}
