// package mongodb provides a wrapped MongoDB client with high-level operations for managing
// databases, collections, and documents using the official MongoDB Go driver. [Built at Machani Robotics]
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/machanirobotics/loom/go/mongodb/shared"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoOperations aggregates all MongoDB operation interfaces, including
// database, schema, collection, document, and import/export management.
type MongoOperations interface {
	DatabaseManager
	DatabaseSchemaManager
	CollectionManager
	DocumentWriter
	DataImporterExporter

	// SetDatabaseName configures the target database for operations.
	SetDatabaseName(dbName string)
	// SetCollectionName configures the target collection for operations.
	SetCollectionName(collName string)
	// CreateCollection creates the configured collection.
	CreateCollection() error
	// DropCollection drops the configured collection.
	DropCollection() error
	// GetHealth performs a ping to verify the MongoDB server is reachable.
	GetHealth() error
	// Close disconnects the MongoDB client, cleaning up resources.
	Close() error
	// GetCurrentDatabaseandCollection returns the names of the configured database and collection.
	GetCurrentDatabaseandCollection() (string, string)
}

// MongoDBClient is a wrapper around the MongoDB client and database,
// providing configuration and management methods for databases and collections.
type MongoDBClient struct {
	client   *mongo.Client
	database *mongo.Database
	Set      MongoDBSet
}

// NewMongoDBClient creates and initializes a MongoDB client using the provided options.
// It sets up authentication if credentials are supplied, connects to the server,
// and verifies connectivity by pinging the MongoDB instance.
func NewMongoDBClient(opts MongoDBClientOptions) (MongoOperations, error) {
	// Prepend scheme if missing.
	if !strings.HasPrefix(opts.DatabaseURL, "mongodb://") && !strings.HasPrefix(opts.DatabaseURL, "mongodb+srv://") {
		opts.DatabaseURL = "mongodb://" + opts.DatabaseURL
	}

	// Inject credentials if provided.
	if opts.Auth.Username != "" && opts.Auth.Password != "" {
		host := strings.TrimPrefix(opts.DatabaseURL, "mongodb://")
		opts.DatabaseURL = fmt.Sprintf("mongodb://%s:%s@%s",
			opts.Auth.Username,
			opts.Auth.Password,
			host,
		)
	}

	// Establish connection to MongoDB server.
	clientOpts := options.Client().ApplyURI(opts.DatabaseURL)
	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB at %q: %w", opts.DatabaseURL, err)
	}

	// Verify connectivity via ping.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("unable to ping MongoDB: %w", err)
	}

	shared.Pulse.Logger.Debug("Connected to MongoDB with URL", opts.DatabaseURL)
	return &MongoDBClient{client: client}, nil
}

// guardNames ensures that the database (and optionally the collection) name is set
// before attempting operations that require them.
func (m *MongoDBClient) guardNames(requireCollection bool) error {
	if m.Set.DatabaseName == "" {
		return errors.New("database name is not set")
	}
	if requireCollection && m.Set.CollectionName == "" {
		return errors.New("collection name is not set")
	}
	return nil
}

// SetDatabaseName sets the current database on the client.
// If the database does not exist, subsequent operations like CreateCollection
// can be used to initialize it.
func (m *MongoDBClient) SetDatabaseName(dbName string) {
	shared.Pulse.Logger.Debug("Setting database:", dbName)
	m.Set.DatabaseName = dbName
	m.database = m.client.Database(dbName)
}

// SetCollectionName sets the current collection name on the client.
func (m *MongoDBClient) SetCollectionName(collName string) {
	shared.Pulse.Logger.Debug("Setting collection:", collName)
	m.Set.CollectionName = collName
}

// GetHealth checks the MongoDB server health by sending a ping.
func (m *MongoDBClient) GetHealth() error {
	if err := m.guardNames(false); err != nil {
		shared.Pulse.Logger.Error("Health check failed due to missing config", err)
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.client.Ping(ctx, nil); err != nil {
		shared.Pulse.Logger.Error("MongoDB health check failed", err)
		return fmt.Errorf("unable to ping MongoDB: %w", err)
	}
	shared.Pulse.Logger.Debug("MongoDB client is healthy")
	return nil
}

// Close disconnects the MongoDB client and cleans up resources.
func (m *MongoDBClient) Close() error {
	if err := m.guardNames(false); err != nil {
		shared.Pulse.Logger.Error("Close failed due to missing config", err)
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.client.Disconnect(ctx); err != nil {
		shared.Pulse.Logger.Error("Failed to disconnect MongoDB client", err)
		return fmt.Errorf("failed to disconnect from MongoDB: %w", err)
	}
	shared.Pulse.Logger.Debug("MongoDB client disconnected")
	return nil
}

// GetCurrentDatabaseandCollection returns the configured database and collection names.
// If either is missing, it logs a warning and returns empty strings.
func (m *MongoDBClient) GetCurrentDatabaseandCollection() (string, string) {
	if err := m.guardNames(true); err != nil {
		shared.Pulse.Logger.Warn("Current names not set", err)
		return "", ""
	}
	shared.Pulse.Logger.Debugf("Current database and collection are %s and %s", m.Set.DatabaseName, m.Set.CollectionName)
	return m.Set.DatabaseName, m.Set.CollectionName
}
