package options

import (
	"errors"

	"github.com/machanirobotics/loom/go/nats/shared"
	"github.com/nats-io/nats.go"
)

// Validate verifies the NatsClientOptions and returns a validated copy plus
// the derived nats.Options slice. If fields are empty, defaults are merged
// from loadDefaultConnectionOptions().
func (o NatsClientOptions) Validate() (NatsClientOptions, []nats.Option, error) {
	// Load defaults from environment or built-ins.
	defaults := loadDefaultConnectionOptions()

	// Merge defaults where fields are missing.
	if o.URL == "" {
		o.URL = defaults.URL
		shared.Pulse.Logger.Debug("URL not provided, using default")
	}
	if o.Name == "" {
		o.Name = defaults.Name
		shared.Pulse.Logger.Debug("Name not provided, using default")
	}
	if o.Auth.Username == "" {
		o.Auth.Username = defaults.Auth.Username
		if o.Auth.Username != "" {
			shared.Pulse.Logger.Debug("Username not provided, using default")
		}
	}
	if o.Auth.Password == "" {
		o.Auth.Password = defaults.Auth.Password
		if o.Auth.Password != "" {
			shared.Pulse.Logger.Debug("Password not provided, using default")
		}
	}

	// Required: URL must not be empty.
	if o.URL == "" {
		shared.Pulse.Logger.Error("server URL is not set (required)")
		return NatsClientOptions{}, nil, errors.New("nats server url must be provided")
	}

	// Build NATS connection options.
	var connOpts []nats.Option
	connOpts = append(connOpts, nats.Name(o.Name))

	// Auth handling.
	switch {
	case o.Auth.Username == "" && o.Auth.Password == "":
		// No auth, allowed if server permits it.
		shared.Pulse.Logger.Debug("no authentication provided")
	case o.Auth.Username == "" || o.Auth.Password == "":
		shared.Pulse.Logger.Warn("partial authentication provided; both username and password should be set")
	default:
		connOpts = append(connOpts, nats.UserInfo(o.Auth.Username, o.Auth.Password))
		shared.Pulse.Logger.Debugf("using username/password authentication (user=%s)", o.Auth.Username)
	}

	return o, connOpts, nil
}
