// Package arangodb provides a client for interacting with ArangoDB.
// It includes functionality for managing databases, collections, documents, and graphs.
// The client supports operations such as creating, reading, updating, and deleting documents,
// as well as managing collections and databases. It uses the ArangoDB Go driver for communication
// with the ArangoDB server and provides a structured interface for database operations.
package arangodb

import (
	"context"
	"log"
	"time"

	"github.com/arangodb/go-driver/v2/arangodb"
	"github.com/arangodb/go-driver/v2/connection"
	"github.com/machanirobotics/loom/go/arangodb/options"
	"github.com/machanirobotics/loom/go/arangodb/shared"
)

// MongoOperations aggregates all ArangoDB operation interfaces, including
// database, schema, collection, document, and import/export management.
type ArangoDB interface {
	// GetHealth checks the health of the ArangoDB client by sending a Health Check Request.
	// Returns nil if the client is healthy, or an error if the health check fails.
	GetHealth() error

	// Close terminates the ArangoDB client connection and releases any associated resources.
	Close() error

	// DatabaseManager provides methods for managing databases.
	DatabaseManager
}

// ArangoDBClient is a wrapper around the ArangoDB client and database,
// providing configuration and management methods for databases and collections.
type ArangoDBClient struct {
	httpConn connection.Connection
	client   arangodb.Client
	db       arangodb.Database
	_        arangodb.Collection // Reserved for future use
	_        arangodb.Graph      // Reserved for future use
}

// NewArangoDBClient creates and initializes a ArangoDB client using the provided options.
// It sets up authentication if credentials are supplied, connects to the server,
// and verifies connectivity by pinging the ArangoDB instance.
func NewArangoDBClient(opts options.ArangoDBClientOptions) (ArangoDB, error) {
	if err := validateConfig(opts); err != nil {
		return nil, err
	}

	httpConn := connection.NewHttpConnection(
		connection.HttpConfiguration{
			Endpoint: connection.NewRoundRobinEndpoints(opts.Endpoints),
		},
	)
	// Add authentication
	auth := connection.NewBasicAuth("root", "root")
	err := httpConn.SetAuthentication(auth)
	if err != nil {
		log.Fatalf("Failed to set authentication: %v", err)
	}

	shared.Pulse.Logger.Debugf("ArangoDB client: created HTTP/1.1 connection with endpoints: %v", opts.Endpoints)

	applyConnectionTimeout(httpConn, opts.ConnectionTimeout)

	arangoClient := arangodb.NewClient(httpConn)

	if err := checkHealth(context.Background(), arangoClient, httpConn.GetEndpoint().List()[0]); err != nil {
		return nil, err
	}

	return &ArangoDBClient{
		client:   arangoClient,
		httpConn: httpConn,
	}, nil
}

// GetHealth checks the ArangoDB server health by sending a Health Check Request.
func (m *ArangoDBClient) GetHealth() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := checkHealth(ctx, m.client, m.httpConn.GetEndpoint().List()[0]); err != nil {
		shared.Pulse.Logger.Error("Failed to get health of ArangoDB client", err)
		return err
	}
	shared.Pulse.Logger.Debug("ArangoDB client is healthy")
	return nil
}

func (m *ArangoDBClient) Close() error {
	shared.Pulse.Logger.Debug("Closing ArangoDB client connection")
	return shared.Close()
}
