package arangodb

import (
	"context"
	"fmt"

	"github.com/arangodb/go-driver/v2/arangodb"
	"github.com/arangodb/go-driver/v2/connection"
	"github.com/machanirobotics/loom/go/arangodb/options"
	"github.com/machanirobotics/loom/go/arangodb/shared"
)

// validateConfig checks if the provided client options are valid.
// It currently ensures that at least one endpoint is specified.
func validateConfig(config options.ArangoDBClientOptions) error {
	if len(config.Endpoints) == 0 {
		err := fmt.Errorf("ArangoDB client: no endpoints provided")
		shared.Pulse.Logger.Error(err.Error())
		return err
	}
	shared.Pulse.Logger.Debugf("ArangoDB client: initializing with endpoints: %v", config.Endpoints)
	return nil
}

// applyConnectionTimeout sets a queue timeout on the given HTTP connection
// if the timeout value is greater than zero.
func applyConnectionTimeout(httpConn connection.Connection, timeout uint) {
	if timeout > 0 {
		shared.Pulse.Logger.Debugf("ArangoDB client: applying connection timeout: %d seconds", timeout)
		httpConn.SetConfiguration(connection.ArangoDBConfiguration{
			ArangoQueueTimeoutEnabled: true,
			ArangoQueueTimeoutSec:     5, // Note: This is a hardcoded value
		})
	}
}

// checkHealth verifies the availability of a specific ArangoDB endpoint.
// It returns an error if the connection health check fails.
func checkHealth(ctx context.Context, client arangodb.Client, endpoint string) error {
	health := client.CheckAvailability(ctx, endpoint)
	if health != nil {
		shared.Pulse.Logger.Errorf("ArangoDB client: connection to %s failed: %v", endpoint, health.Error())
		return fmt.Errorf("ArangoDB client: failed to connect to %s: %w", endpoint, health)
	}
	shared.Pulse.Logger.Debugf("ArangoDB client: successfully connected to %s", endpoint)
	return nil
}
