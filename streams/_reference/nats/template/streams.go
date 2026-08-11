package template

import (
	"github.com/machanirobotics/loom/go/nats/options"
	"github.com/machanirobotics/loom/go/nats/types"
)

// RegularStream builds a file-backed stream config. Zero MaxAge and MaxBytes means unbounded.
func RegularStream(name string, subjects []string, retention options.RegularStreamRetention) types.StreamConfig {
	return types.StreamConfig{
		Name:     name,
		Subjects: subjects,
		Storage:  types.FileStorage,
		Replicas: 1,
		MaxAge:   retention.MaxAge,
		MaxBytes: retention.MaxBytes,
	}
}
