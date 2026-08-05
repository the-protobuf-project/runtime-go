package redis

import (
	"encoding/json"
	"fmt"
)

// encode turns a caller's value into the bytes stored on the server. Any
// JSON-serializable value works, so a model gaining a field is not a change
// here.
func encode(value any) ([]byte, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("cache/redis: cannot encode value: %w", err)
	}
	return b, nil
}

// decode unmarshals stored bytes into dest, which must be a non-nil pointer.
func decode(data []byte, dest any) error {
	if dest == nil {
		return fmt.Errorf("cache/redis: decode destination is nil")
	}
	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("cache/redis: cannot decode stored value: %w", err)
	}
	return nil
}
