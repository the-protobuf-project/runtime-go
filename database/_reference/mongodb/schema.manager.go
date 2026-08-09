package mongodb

import (
	"context"
	"fmt"
	"time"

	"github.com/machanirobotics/loom/go/mongodb/shared"
)

// DatabaseSchemaManager defines operations for managing MongoDB collection-level schemas.
type DatabaseSchemaManager interface {
	// CreateSchema creates a new collection in the configured database.
	CreateSchema(schema interface{}) error
	// DropSchema drops the configured collection from the database.
	DropSchema(filter interface{}) error
	// ListSchemas returns all collection names in the configured database.
	ListSchemas() ([]string, error)
	// UpdateSchema updates a single document in the configured collection.
	UpdateSchema(schema interface{}) error
	// ValidateSchema checks if a document matching schema exists in the configured collection.
	ValidateSchema(schema interface{}) (bool, error)
}

// CreateSchema creates a new collection in the current database.
//
// It logs the action, attempts to create the collection named by
// m.Set.CollectionName under m.Set.DatabaseName, and returns any error.
func (m *MongoDBClient) CreateSchema(schema interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	target := fmt.Sprintf("%s.%s", m.Set.DatabaseName, m.Set.CollectionName)
	shared.Pulse.Logger.Debug("Creating schema", target)

	if err := m.client.Database(m.Set.DatabaseName).
		CreateCollection(ctx, m.Set.CollectionName); err != nil {
		shared.Pulse.Logger.Errorf("Failed to create schema %s: %v", target, err)
		return err
	}

	shared.Pulse.Logger.Debug("Successfully created schema", target)
	return nil
}

// DropSchema drops the configured collection from the current database.
//
// The filter parameter is ignored here (collection‐level drop only),
// but kept for interface compatibility.
func (m *MongoDBClient) DropSchema(filter interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	target := fmt.Sprintf("%s.%s", m.Set.DatabaseName, m.Set.CollectionName)
	shared.Pulse.Logger.Debug("Dropping schema", target)

	if err := m.client.Database(m.Set.DatabaseName).
		Collection(m.Set.CollectionName).
		Drop(ctx); err != nil {
		shared.Pulse.Logger.Errorf("Failed to drop schema %s: %v", target, err)
		return err
	}

	shared.Pulse.Logger.Debug("Successfully dropped schema", target)
	return nil
}

// ListSchemas returns the names of all collections in the current database.
//
// It logs the database name and the count of collections.
func (m *MongoDBClient) ListSchemas() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shared.Pulse.Logger.Debug("Listing schemas", m.Set.DatabaseName)
	cols, err := m.client.Database(m.Set.DatabaseName).
		ListCollectionNames(ctx, struct{}{})
	if err != nil {
		shared.Pulse.Logger.Errorf("Failed to list schemas for database %s: %v", m.Set.DatabaseName, err)
		return nil, err
	}

	shared.Pulse.Logger.Debug(fmt.Sprintf("Schemas listed for %s", m.Set.DatabaseName), len(cols))
	return cols, nil
}

// UpdateSchema updates a single document in the configured collection.
//
// The provided schema is treated as the replacement document.
func (m *MongoDBClient) UpdateSchema(schema interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	target := fmt.Sprintf("%s.%s", m.Set.DatabaseName, m.Set.CollectionName)
	shared.Pulse.Logger.Debug("Updating schema", target)

	if _, err := m.client.Database(m.Set.DatabaseName).
		Collection(m.Set.CollectionName).
		UpdateOne(ctx, struct{}{}, schema); err != nil {
		shared.Pulse.Logger.Errorf("Failed to update schema %s: %v", target, err)
		return err
	}

	shared.Pulse.Logger.Debug("Successfully updated schema", target)
	return nil
}

// ValidateSchema checks whether a document matching the provided filter exists.
//
// Returns true if found, false (and an error) otherwise.
func (m *MongoDBClient) ValidateSchema(schema interface{}) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	target := fmt.Sprintf("%s.%s", m.Set.DatabaseName, m.Set.CollectionName)
	shared.Pulse.Logger.Debug("Validating schema", target)

	result := m.client.Database(m.Set.DatabaseName).
		Collection(m.Set.CollectionName).
		FindOne(ctx, schema)
	if err := result.Err(); err != nil {
		shared.Pulse.Logger.Errorf("Schema validation failed for %s: %v", target, err)
		return false, err
	}

	shared.Pulse.Logger.Debug("Schema validation successful", target)
	return true, nil
}
