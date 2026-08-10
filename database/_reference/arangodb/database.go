package arangodb

import (
	"context"
	"fmt"
	"time"

	"github.com/arangodb/go-driver/v2/arangodb"
	"github.com/machanirobotics/loom/go/arangodb/options"
	"github.com/machanirobotics/loom/go/arangodb/shared"
)

// DatabaseManager defines a set of methods for managing databases within an ArangoDB instance.
// It handles creation, retrieval, modification, and deletion of databases.
// Each operation is performed with a 5-second timeout.
type DatabaseManager interface {
	// CreateDatabase creates a new database with the specified name.
	// If forceOverride is true, an existing database with the same name
	// will be deleteped before creation.
	// Returns an error if the database cannot be created or if it already
	// exists and forceOverride is false.
	CreateDatabase(name string) (*options.DatabaseInfo, error)

	// GetDatabase retrieves an existing database metadata.
	// Returns the database instance and an error if the database does not exist.
	GetDatabase(name string) (*options.DatabaseInfo, error)

	// SetDatabase sets the current database for the client.
	// Returns the database instance and an error if the database does not exist.
	SetDatabase(name string) (ArangoDBManager, error)

	// DeleteDatabase removes the specified database from the ArangoDB instance.
	// Returns an error if the database cannot be deleteped or does not exist.
	DeleteDatabase(name string) error

	// ListDatabases retrieves the names of all databases present in the
	// ArangoDB instance.
	// Returns a slice of database names and nil on success, or nil and an error
	// if the databases cannot be listed.
	ListDatabases() ([]*options.DatabaseInfo, error)
}

// CreateDatabase creates a new database with the specified `name`.
//
// If `forceOverride` is true, and a database with `name` already exists,
// it will be deleteped before a new one is created. If `forceOverride` is false
// and the database already exists, the operation will be skipped without error.
//
// This function initializes the database by ensuring its creation and sets
// the internal `m.db` field to the newly created database instance if successful.
func (m *ArangoDBClient) CreateDatabase(name string) (*options.DatabaseInfo, error) {
	shared.Pulse.Logger.Debug("Attempting to create database", name)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create the database
	db, err := m.client.CreateDatabase(ctx, name, nil)
	if err != nil {
		shared.Pulse.Logger.Debugf("Failed to create database %+v", err)
		return nil, fmt.Errorf("failed to create database '%s': %w", name, err)
	}

	// Get the database info to ensure it was created successfully
	dbInfo, err := db.Info(ctx)
	if err != nil {
		// This error path is less likely if CreateDatabase succeeded, but good for robustness
		shared.Pulse.Logger.Error("Failed to get database info after creation", name, "error", err)
		return nil, fmt.Errorf("failed to get database info for '%s' after creation: %w", name, err)
	}

	// Set the current database to the newly created one
	m.db = db

	shared.Pulse.Logger.Debug("Successfully created database", dbInfo.Name)
	return &dbInfo, nil
}

// GetDatabase retrieves an existing ArangoDB database instance by its `name`.
//
// This function attempts to retrieve a database using the underlying ArangoDB client.
// It uses a context with a 5-second timeout for the operation.
func (m *ArangoDBClient) GetDatabase(name string) (*options.DatabaseInfo, error) {
	shared.Pulse.Logger.Debug("Attempting to get database", name)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	database, err := m.client.GetDatabase(ctx, name, &arangodb.GetDatabaseOptions{SkipExistCheck: false})
	if err != nil {
		shared.Pulse.Logger.Error("Failed to get database", name, "error", err)
		return nil, fmt.Errorf("failed to get database '%s': %w", name, err)
	}

	info, err := database.Info(ctx) // Ensure the database is initialized and accessible
	if err != nil {
		shared.Pulse.Logger.Error("Failed to retrieve database info", name, "error", err)
		return nil, fmt.Errorf("failed to retrieve info for database '%s': %w", name, err)
	}

	shared.Pulse.Logger.Debug("Successfully retrieved database", name)
	return &info, nil
}

// ListDatabases retrieves the names of all databases present in the ArangoDB instance.
//
// This function queries the ArangoDB client for a list of all databases
// and extracts their names. It uses a context with a 5-second timeout.
func (m *ArangoDBClient) ListDatabases() ([]*options.DatabaseInfo, error) {
	shared.Pulse.Logger.Debug("Attempting to list all databases")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	databases, err := m.client.Databases(ctx)
	if err != nil {
		shared.Pulse.Logger.Error("Failed to list databases", "error", err)
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}

	var dbNames []*options.DatabaseInfo
	for _, db := range databases {
		info, err := db.Info(ctx)
		if err != nil {
			shared.Pulse.Logger.Error("Failed to get database info", db.Name, "error", err)
			continue // Skip this database if we can't get its info
		}
		dbNames = append(dbNames, &info)
	}

	shared.Pulse.Logger.Debug("Successfully listed databases", "count", len(dbNames))
	return dbNames, nil
}

// SetDatabase sets the current database on the client.
//
// This function retrieves a database by its name and sets it as the current database
// for subsequent operations. If the database does not exist, it returns an error.
func (m *ArangoDBClient) SetDatabase(name string) (ArangoDBManager, error) {
	shared.Pulse.Logger.Debug("Attempting to set database", name)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	database, err := m.client.GetDatabase(ctx, name, &arangodb.GetDatabaseOptions{SkipExistCheck: false})
	if err != nil {
		shared.Pulse.Logger.Error("Failed to set database", name, "error", err)
		// When returning an error, you must return the zero value for the struct.
		return ArangoDBManager{}, fmt.Errorf("failed to set database '%s': %w", name, err)
	}

	m.db = database
	shared.Pulse.Logger.Debug("Successfully set database", m.db.Name())

	// 1. Create the wrapper which implements the GraphManager interface.
	wrapper := &arangoInterfaceWrapper{Database: database}

	// 2. Construct the ArangoDBManager struct and place the wrapper in the correct field.
	return ArangoDBManager{
		Graph:      wrapper,
		Collection: wrapper,
	}, nil
}

// DeleteDatabase removes the specified database from the ArangoDB instance.
//
// This function first checks if the database exists using `GetDatabase` and then
// attempts to remove it. It uses a context with a 5-second timeout.
func (m *ArangoDBClient) DeleteDatabase(name string) error {
	shared.Pulse.Logger.Debug("Attempting to delete database", name)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	checkDB, err := m.client.GetDatabase(ctx, name, &arangodb.GetDatabaseOptions{SkipExistCheck: false})
	if err != nil {
		shared.Pulse.Logger.Error("Failed to find database to delete", name, "error", err)
		return fmt.Errorf("failed to find database '%s' for deleteping: %w", name, err)
	}

	if err := checkDB.Remove(ctx); err != nil {
		shared.Pulse.Logger.Error("Failed to delete database", name, "error", err)
		return fmt.Errorf("failed to delete database '%s': %w", name, err)
	}

	shared.Pulse.Logger.Debug("Successfully deleteped database", name)
	return nil
}
