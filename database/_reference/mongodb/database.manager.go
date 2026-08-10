package mongodb

import (
	"context"
	"fmt"
	"time"

	"github.com/machanirobotics/loom/go/mongodb/shared"
)

// DatabaseManager handles database-level operations, including creation,
// deletion, and listing of databases. It provides a simplified interface
// for managing MongoDB databases, allowing users to create new databases,
// drop existing ones, and retrieve a list of all databases in the MongoDB
// instance.
type DatabaseManager interface {
	// CreateDatabase creates a new database with the specified name.
	// It initializes the database by creating a dummy collection.
	CreateDatabase() error
	// DropDatabase removes the specified database.
	DropDatabase() error
	// ListDatabases retrieves the names of all databases.
	ListDatabases() ([]string, error)
}

// CreateDatabase sets the current database to `databaseName`, creates
// an "init_collection" if needed, and returns any error.
func (m *MongoDBClient) CreateDatabase() error {
	shared.Pulse.Logger.Debug("Creating database", m.Set.DatabaseName)

	// 1) Point m.database at this new database
	m.database = m.client.Database(m.Set.DatabaseName)

	// 2) Attempt to create a dummy collection so the DB “exists”
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := m.database.CreateCollection(ctx, "init_collection"); err != nil {
		shared.Pulse.Logger.Errorf("Failed to create initial collection for database %s: %v", m.Set.DatabaseName, err)
		return fmt.Errorf("failed to create initial collection: %w", err)
	}

	shared.Pulse.Logger.Debug("Successfully created database", m.Set.DatabaseName)
	return nil
}

// DropDatabase removes the specified database.
func (m *MongoDBClient) DropDatabase() error {
	shared.Pulse.Logger.Debug("Dropping database", m.Set.DatabaseName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := m.client.Database(m.Set.DatabaseName).Drop(ctx); err != nil {
		shared.Pulse.Logger.Errorf("Failed to drop database %s: %v", m.Set.DatabaseName, err)
		return err
	}

	shared.Pulse.Logger.Debug("Successfully dropped database", m.Set.DatabaseName)
	return nil
}

// ListDatabases retrieves the names of all databases.
func (m *MongoDBClient) ListDatabases() ([]string, error) {
	shared.Pulse.Logger.Debug("Listing databases")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	databases, err := m.client.ListDatabaseNames(ctx, struct{}{})
	if err != nil {
		shared.Pulse.Logger.Errorf("Failed to list databases: %v", err)
		return nil, err
	}

	shared.Pulse.Logger.Debug("Databases listed", len(databases))
	return databases, nil
}
