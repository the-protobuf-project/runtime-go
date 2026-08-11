package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// normalizeToJSON validates and converts an input value into a consistent format:
// a JSON byte slice and a map[string]interface{}.
//
// It accepts the following input types:
//   - map[string]interface{}: Used directly.
//   - map[string]string: Converted to map[string]interface{}.
//   - string: Must be a valid JSON object.
//   - struct (or pointer to struct): Must have at least one exported field with a `json` tag.
//
// All other types (slices, primitives, etc.) will result in an error. This function
// acts as a strict gateway to ensure data is well-formed before storage.
func normalizeToJSON(v interface{}) ([]byte, map[string]interface{}, error) {
	if v == nil {
		return nil, nil, fmt.Errorf("content cannot be nil")
	}

	switch t := v.(type) {
	case map[string]interface{}:
		b, err := json.Marshal(t)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal content: %w", err)
		}
		return b, t, nil

	case map[string]string:
		// Convert to map[string]interface{} for a consistent return type.
		m := make(map[string]interface{}, len(t))
		for k, val := range t {
			m[k] = val
		}
		b, err := json.Marshal(m)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal content: %w", err)
		}
		return b, m, nil

	case string:
		// The string must be a valid JSON object.
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(t), &m); err != nil {
			return nil, nil, fmt.Errorf("string content is not a valid JSON object: %w", err)
		}
		// Re-marshal to ensure consistent formatting.
		b, _ := json.Marshal(m)
		return b, m, nil

	default:
		// For structs, use reflection to validate and convert.
		rv := reflect.ValueOf(v)
		rt := rv.Type()

		if rv.Kind() == reflect.Ptr {
			if rv.IsNil() {
				return nil, nil, fmt.Errorf("content cannot be a nil pointer")
			}
			rv = rv.Elem()
			rt = rv.Type()
		}

		if rv.Kind() == reflect.Struct {
			// Enforce a contract: structs must be explicitly tagged for JSON serialization.
			if !hasAnyJSONTag(rt) {
				return nil, nil, fmt.Errorf("struct of type %v has no json tags and is not supported", rt)
			}
			// Use the standard marshal/unmarshal trick to convert the struct to a map.
			b, err := json.Marshal(v)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to marshal struct: %w", err)
			}
			var m map[string]interface{}
			if err := json.Unmarshal(b, &m); err != nil {
				// This should be unlikely if the initial marshal succeeded.
				return nil, nil, fmt.Errorf("marshaled struct could not be converted to a map: %w", err)
			}
			return b, m, nil
		}

		// Explicitly reject all other types.
		return nil, nil, fmt.Errorf("unsupported content type %T; must be a map, a JSON object string, or a tagged struct", v)
	}
}

// hasAnyJSONTag uses reflection to check if a struct type has at least one
// exported field with a `json` tag that is not "-". It recursively checks
// embedded anonymous structs.
func hasAnyJSONTag(t reflect.Type) bool {
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		// Skip unexported fields.
		if sf.PkgPath != "" {
			continue
		}

		// Recurse into embedded structs.
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct {
			if hasAnyJSONTag(sf.Type) {
				return true
			}
		}

		tag := sf.Tag.Get("json")
		if tag == "" {
			continue
		}

		// Check if the tag is substantive and not explicitly ignored.
		name := strings.Split(tag, ",")[0]
		if name != "" && name != "-" {
			return true
		}
	}
	return false
}

// readDocAsMap retrieves a document from Redis by its ID and unmarshals it
// into a map[string]interface{}. It provides a specific error if the document
// is not found or if its content is not a valid JSON object.
func (c *Client) readDocAsMap(ctx context.Context, id string) (map[string]interface{}, error) {
	rdb := c.redisClient
	val, err := rdb.Get(ctx, kDoc(id)).Bytes()

	if err != nil {
		return nil, fmt.Errorf("failed to read document %s: %w", id, err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(val, &m); err != nil {
		return nil, fmt.Errorf("stored document %s is not a valid JSON object: %w", id, err)
	}
	return m, nil
}
