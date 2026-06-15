package shared

import (
	"fmt"
	"sync"

	"github.com/machanirobotics/pulse/pulse-go"
)

var (
	Pulse *pulse.Pulse
	once  sync.Once
)

func init() {
	once.Do(func() {
		p, err := pulse.New().
			WithService("runtime-go-grpc", "1.0.0").
			WithLogLevel(pulse.ModuleLevel_2).WithTracing().
			Build()
		if err != nil {
			fmt.Printf("ERROR: Failed to create Pulse: %v\n", err)
			panic(err)
		}

		Pulse = p
	})
}

// Close should be called by the main application on shutdown
func Close() error {
	if Pulse != nil {
		return Pulse.Close()
	}
	return nil
}
