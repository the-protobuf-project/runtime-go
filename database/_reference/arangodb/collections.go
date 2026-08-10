package arangodb

import (
	"context"
	"fmt"
	"time"

	"github.com/arangodb/go-driver/v2/arangodb"
	"github.com/machanirobotics/loom/go/arangodb/options"
	"github.com/machanirobotics/loom/go/arangodb/shared"
)

// CollectionManager defines a set of methods for managing collections within a database.
// It handles creation, retrieval, modification, and deletion of collections. Each
// operation is performed with a 5-second timeout.
type CollectionManager interface {
	// CreateCollection creates a new collection with a given name then type and schema options.
	// On success, it returns the properties of the newly created collection.
	CreateCollection(name string, collectionType options.CollectionType, opts options.CollectionSchemaOptions) (*options.CollectionProperties, error)

	// GetCollection retrieves the properties of a single collection by its name.
	GetCollection(name string) (*options.CollectionProperties, error)

	// SetCollection sets the active collection for the manager by its name. It returns
	// a new ArangoDBManager instance configured to use the specified collection.
	SetCollection(name string) (ArangoDBManager, error)

	// DeleteCollection removes a collection from the database by its name.
	DeleteCollection(name string) error

	// UpdateCollection modifies the schema of an existing collection. On success, it
	// returns the updated properties of the collection.
	UpdateCollection(name string, opts options.CollectionSchemaOptions) (*options.CollectionProperties, error)

	// ListCollections retrieves the properties of all non-system collections in the database.
	ListCollections() ([]*options.CollectionProperties, error)
}

// CreateCollection creates a new collection with a given name and schema options.
func (m *arangoInterfaceWrapper) CreateCollection(name string, collectionType options.CollectionType, opts options.CollectionSchemaOptions) (*options.CollectionProperties, error) {
	shared.Pulse.Logger.Debug("Entering CreateCollection", name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	colltype := arangodb.CollectionType(collectionType)

	collection, err := m.CreateCollectionV2(ctx, name, &arangodb.CreateCollectionPropertiesV2{
		Schema: &arangodb.CollectionSchemaOptions{
			Rule:    opts.Schema,
			Level:   arangodb.CollectionSchemaLevel(opts.Level),
			Message: opts.Description,
		},
		Type: &colltype,
	})
	if err != nil {
		shared.Pulse.Logger.Error("Failed to create collection", "details", fmt.Sprintf("name: %s, error: %v", name, err))
		return nil, fmt.Errorf("failed to create collection '%s': %w", name, err)
	}

	m.Collection = collection
	properties, err := collection.Properties(ctx)
	if err != nil {
		shared.Pulse.Logger.Error("Failed to get properties after creating collection", "details", fmt.Sprintf("name: %s, error: %v", name, err))
		return nil, fmt.Errorf("failed to get properties for new collection '%s': %w", name, err)
	}

	shared.Pulse.Logger.Debug("CreateCollection successful", name)
	return &properties, nil
}

// GetCollection retrieves the properties of a single collection by its name.
func (m *arangoInterfaceWrapper) GetCollection(name string) (*options.CollectionProperties, error) {
	shared.Pulse.Logger.Debug("Entering GetCollection", name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collection, err := m.Database.GetCollection(ctx, name, &arangodb.GetCollectionOptions{})
	if err != nil {
		shared.Pulse.Logger.Error("Failed to get collection", "details", fmt.Sprintf("name: %s, error: %v", name, err))
		return nil, fmt.Errorf("failed to get collection '%s': %w", name, err)
	}

	properties, err := collection.Properties(ctx)
	if err != nil {
		shared.Pulse.Logger.Error("Failed to get collection properties", "details", fmt.Sprintf("name: %s, error: %v", name, err))
		return nil, fmt.Errorf("failed to get properties for collection '%s': %w", name, err)
	}

	shared.Pulse.Logger.Debug("GetCollection successful", name)
	return &properties, nil
}

// ListCollections retrieves the properties of all non-system collections in the database.
func (m *arangoInterfaceWrapper) ListCollections() ([]*options.CollectionProperties, error) {
	shared.Pulse.Logger.Debug("Entering ListCollections", m.Database.Name())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collections, err := m.Collections(ctx)
	if err != nil {
		shared.Pulse.Logger.Error("Failed to list collections", err)
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}

	var collectionProps []*options.CollectionProperties
	for _, col := range collections {
		props, err := col.Properties(ctx)
		if err != nil {
			shared.Pulse.Logger.Warn("Failed to get properties for collection, skipping", "details", fmt.Sprintf("name: %s, error: %v", col.Name(), err))
			continue
		}
		if !props.IsSystem {
			collectionProps = append(collectionProps, &props)
		}
	}

	shared.Pulse.Logger.Debug("ListCollections successful count", len(collectionProps))
	return collectionProps, nil
}

// SetCollection gets a specific collection by name and returns a new, fully configured
// ArangoDBManager ready for operations on that collection.
func (m *arangoInterfaceWrapper) SetCollection(name string) (ArangoDBManager, error) {
	shared.Pulse.Logger.Debug("Entering SetCollection", name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collection, err := m.Database.GetCollection(ctx, name, &arangodb.GetCollectionOptions{})
	if err != nil {
		shared.Pulse.Logger.Error("Failed to set collection", fmt.Sprintf("name: %s, error: %v", name, err))
		return ArangoDBManager{}, fmt.Errorf("failed to set collection '%s': %w", name, err)
	}

	// Create a new wrapper that holds references to both the database and the specific collection.
	scopedWrapper := &arangoInterfaceWrapper{
		Database:   m.Database,
		Collection: collection,
	}

	shared.Pulse.Logger.Debug("SetCollection successful", name)

	// Return a new, complete ArangoDBManager with all its manager fields initialized.
	return ArangoDBManager{
		Collection: scopedWrapper,
		Document:   scopedWrapper,
		Graph:      scopedWrapper, // Assuming arangoInterfaceWrapper also implements GraphManager
	}, nil
}

// DeleteCollection removes a collection from the database by its name.
func (m *arangoInterfaceWrapper) DeleteCollection(name string) error {
	shared.Pulse.Logger.Debug("Entering DeleteCollection", name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collection, err := m.Database.GetCollection(ctx, name, &arangodb.GetCollectionOptions{})
	if err != nil {
		shared.Pulse.Logger.Error("Failed to find collection for deletion", "details", fmt.Sprintf("name: %s, error: %v", name, err))
		return fmt.Errorf("failed to find collection '%s' to delete: %w", name, err)
	}

	if err := collection.Remove(ctx); err != nil {
		shared.Pulse.Logger.Error("Failed to delete collection", "details", fmt.Sprintf("name: %s, error: %v", name, err))
		return fmt.Errorf("failed to delete collection '%s': %w", name, err)
	}

	shared.Pulse.Logger.Debug("DeleteCollection successful", name)
	return nil
}

// UpdateCollection modifies the schema of an existing collection.
func (m *arangoInterfaceWrapper) UpdateCollection(name string, opts options.CollectionSchemaOptions) (*options.CollectionProperties, error) {
	shared.Pulse.Logger.Debug("Entering UpdateCollection", name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collection, err := m.Database.GetCollection(ctx, name, &arangodb.GetCollectionOptions{})
	if err != nil {
		shared.Pulse.Logger.Error("Failed to find collection for update", "details", fmt.Sprintf("name: %s, error: %v", name, err))
		return nil, fmt.Errorf("failed to find collection '%s' to update: %w", name, err)
	}

	updateSchema := &arangodb.CollectionSchemaOptions{
		Rule:    opts.Schema,
		Level:   arangodb.CollectionSchemaLevel(opts.Level),
		Message: opts.Description,
	}

	if err := collection.SetPropertiesV2(ctx, arangodb.SetCollectionPropertiesOptionsV2{
		Schema: updateSchema,
	}); err != nil {
		shared.Pulse.Logger.Error("Failed to update collection properties", "details", fmt.Sprintf("name: %s, error: %v", name, err))
		return nil, fmt.Errorf("failed to update collection '%s': %w", name, err)
	}

	properties, err := collection.Properties(ctx)
	if err != nil {
		shared.Pulse.Logger.Error("Failed to get properties after updating collection", "details", fmt.Sprintf("name: %s, error: %v", name, err))
		return nil, fmt.Errorf("failed to get properties for updated collection '%s': %w", name, err)
	}

	shared.Pulse.Logger.Debug("UpdateCollection successful", name)
	return &properties, nil
}
