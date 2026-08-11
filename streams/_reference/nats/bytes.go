package nats

import (
	"encoding/json"
	"fmt"

	"github.com/machanirobotics/loom/go/nats/shared"
)

// convertAnyDataToBytes converts the data in the JetStreamPublishRequest to a byte slice.
// It handles different types of data, including string, []byte, and other types that can be marshaled to JSON.
// If the data is already a byte slice, it returns it directly.
func ConvertAnyDataToBytes(data any) ([]byte, error) {
	switch v := data.(type) {
	case string:
		shared.Pulse.Logger.Debug("ConvertAnyDataToBytes received string; returning raw bytes")
		return []byte(v), nil
	case []byte:
		shared.Pulse.Logger.Debugf("ConvertAnyDataToBytes received []byte (len=%d); returning as-is", len(v))
		return v, nil
	default:
		shared.Pulse.Logger.Debugf("ConvertAnyDataToBytes JSON-marshal type %T", v)
		b, err := json.Marshal(v)
		if err != nil {
			shared.Pulse.Logger.Errorf("ConvertAnyDataToBytes failed to marshal %T: %v", v, err)
			return nil, fmt.Errorf("failed to marshal %T as JSON: %w", v, err)
		}
		return b, nil
	}
}
