package redis

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// canonicalJSON encodes a Go value into a deterministic JSON byte slice.
// The key feature is that map keys are sorted alphabetically before encoding.
// This ensures that the output is always identical for the same data, making it
// suitable for hashing, signatures, or stable comparisons.
func canonicalJSON(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := encodeCanonical(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// encodeCanonical is the recursive helper that writes a canonical JSON
// representation of v into the buffer. It handles various data types and
// ensures map keys are sorted.
func encodeCanonical(buf *bytes.Buffer, v interface{}) error {
	switch x := v.(type) {
	case nil:
		buf.WriteString("null")

	case bool:
		if x {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}

	// For numbers and strings, use the standard library's marshaller to ensure
	// correct formatting and escaping.
	case float64, float32,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		string:
		b, _ := json.Marshal(x)
		buf.Write(b)

	case []interface{}:
		buf.WriteByte('[')
		for i, el := range x {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := encodeCanonical(buf, el); err != nil {
				return err
			}
		}
		buf.WriteByte(']')

	case map[string]interface{}:
		buf.WriteByte('{')
		// Extract, sort, and iterate over keys for deterministic output.
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			// Write the sorted key (as a JSON string).
			kb, _ := json.Marshal(k)
			buf.Write(kb)
			buf.WriteByte(':')
			// Write the corresponding value.
			if err := encodeCanonical(buf, x[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')

	default:
		// For any other type (e.g., custom structs), use a fallback strategy:
		// 1. Marshal to standard JSON.
		// 2. Unmarshal into a generic interface{} (map or slice).
		// 3. Re-run the canonical encoding on the generic type.
		raw, err := json.Marshal(x)
		if err != nil {
			return err
		}
		var tmp interface{}
		if err := json.Unmarshal(raw, &tmp); err != nil {
			return err
		}
		return encodeCanonical(buf, tmp)
	}
	return nil
}

// hashContent generates a stable SHA256 hash for a given map.
// It first serializes the map into canonical JSON to ensure the hash is
// consistent and independent of the map's in-memory key order.
//
// It returns the hex-encoded hash string, the canonical JSON bytes, and an error.
func hashContent(m map[string]interface{}) (string, []byte, error) {
	canon, err := canonicalJSON(m)
	if err != nil {
		return "", nil, fmt.Errorf("could not canonicalize content: %w", err)
	}

	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:]), canon, nil
}
