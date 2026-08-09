package mongodb

import (
	"context"
	"fmt"
	"time"

	"github.com/machanirobotics/loom/go/mongodb/shared"
)

// CollectionManager handles collection-level operations.
// It provides methods to create, drop, and list collections in the current
// database. This interface abstracts the underlying MongoDB operations,
// allowing for easier management of collections without needing to
// directly interact with the MongoDB driver.
type CollectionManager interface {
	CreateCollection() error
	DropCollection() error
	ListCollections() ([]string, error)
}

// CreateCollection creates a new collection with the given name.
func (m *MongoDBClient) CreateCollection() error {
	shared.Pulse.Logger.Debug("Creating collection", m.Set.CollectionName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := m.database.CreateCollection(ctx, m.Set.CollectionName); err != nil {
		shared.Pulse.Logger.Errorf("Failed to create collection %s: %v", m.Set.CollectionName, err)
		return err
	}

	shared.Pulse.Logger.Debug("Successfully created collection", m.Set.CollectionName)
	return nil
}

// DropCollection drops the collection with the given name.
func (m *MongoDBClient) DropCollection() error {
	shared.Pulse.Logger.Debug("Dropping collection", m.Set.CollectionName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := m.database.Collection(m.Set.CollectionName).Drop(ctx); err != nil {
		shared.Pulse.Logger.Errorf("Failed to drop collection %s: %v", m.Set.CollectionName, err)
		return err
	}

	shared.Pulse.Logger.Debug("Successfully dropped collection", m.Set.CollectionName)
	return nil
}

// ListCollections returns the names of all collections in the current database.
func (m *MongoDBClient) ListCollections() ([]string, error) {
	shared.Pulse.Logger.Debug("Listing collections")

	if m.database == nil {
		shared.Pulse.Logger.Error("Database not selected")
		return nil, fmt.Errorf("database not selected; call CreateDatabase or SetDatabaseName first")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collections, err := m.database.ListCollectionNames(ctx, struct{}{})
	if err != nil {
		shared.Pulse.Logger.Errorf("Failed to list collections: %v", err)
		return nil, err
	}

	shared.Pulse.Logger.Debug("Collections listed", len(collections))
	return collections, nil
}
