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
func canonicalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := encodeCanonical(&buf, v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// encodeCanonical is the recursive helper that writes a canonical JSON
// representation of v into the buffer. It handles various data types and
// ensures map keys are sorted.
func encodeCanonical(buf *bytes.Buffer, v any) error {
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

	case []any:
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

	case map[string]any:
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
		// 2. Unmarshal into a generic any (map or slice).
		// 3. Re-run the canonical encoding on the generic type.
		raw, err := json.Marshal(x)
		if err != nil {
			return err
		}
		var tmp any
		if err := json.Unmarshal(raw, &tmp); err != nil {
			return err
		}
		return encodeCanonical(buf, tmp)
	}
	return nil
}

// canonicalize encodes a document payload into its deterministic JSON form and
// the SHA256 hash of that form.
//
// Hashing the canonical encoding rather than the caller's value is what makes
// deduplication reliable: two maps with the same pairs in different insertion
// orders produce identical bytes, and therefore the same hash.
//
// The payload is round-tripped through a generic value first so that structs,
// maps, and pre-encoded JSON all reduce to the same shape — otherwise the same
// record written once as a struct and once as a map would hash differently.
func canonicalize(data any) (hash string, canonical []byte, err error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return "", nil, fmt.Errorf("database/redis: failed to encode document: %w", err)
	}

	var generic any
	if uerr := json.Unmarshal(raw, &generic); uerr != nil {
		return "", nil, fmt.Errorf("database/redis: encoded document is not valid JSON: %w", uerr)
	}

	canonical, err = canonicalJSON(generic)
	if err != nil {
		return "", nil, fmt.Errorf("database/redis: failed to canonicalize document: %w", err)
	}

	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), canonical, nil
}
