package options

import (
	"github.com/machanirobotics/loom/go/nats/helpers"
	"github.com/machanirobotics/loom/go/nats/shared"
)

// loadDefaultConnectionOptions returns a NATS client configuration populated
// from environment variables with sensible defaults.
//
// The following environment variables are read to configure the connection:
//   - NATS_URL:       NATS server URL (default: "nats://localhost:4222")
//   - NATS_NAME:      Client connection name (default: "cutlery-service")
//   - NATS_USERNAME:  Username for authentication (default: "")
//   - NATS_PASSWORD:  Password for authentication (default: "")
//
// If an environment variable is not set, the corresponding default value is
// applied. This allows services to run locally with defaults while supporting
// overrides in production environments.
//
// Example:
//
//	opts := loadDefaultConnectionOptions()
//	// opts.URL = "nats://localhost:4222"
//	// opts.Name = "cutlery-service"
//	// opts.Auth.Username = ""
//	// opts.Auth.Password = ""
func loadDefaultConnectionOptions() NatsClientOptions {
	shared.Pulse.Logger.Warn("loading default connection options from environment")
	return NatsClientOptions{
		URL:  helpers.SetEnvOrDefault("NATS_URL", "nats://localhost:4222"),
		Name: helpers.SetEnvOrDefault("NATS_NAME", "cutlery-service"),
		Auth: NatsAuthOptions{
			Username: helpers.SetEnvOrDefault("NATS_USERNAME", ""),
			Password: helpers.SetEnvOrDefault("NATS_PASSWORD", ""),
		},
		EnableJetStream: true,
	}
}
