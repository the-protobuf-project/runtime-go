package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/machanirobotics/loom/go/mongodb/shared"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// DocumentWriter handles CRUD operations on documents.
type DocumentWriter interface {
	// Insert adds a single document into the specified collection.
	Insert(document interface{}) error
	// Update modifies all documents matching the filter in the specified collection.
	Update(update, filter interface{}) error
	// Delete removes all documents matching the filter from the specified collection.
	Delete(filter interface{}) error
	// Read retrieves all documents matching the filter from the specified collection.
	// Returns a slice of maps (document fields) or an error.
	Read(filter interface{}) ([]map[string]interface{}, error)
}

// Insert adds a document to the given collection.
func (m *MongoDBClient) Insert(document interface{}) error {
	shared.Pulse.Logger.Debug("Inserting document", m.Set.CollectionName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := m.database.Collection(m.Set.CollectionName).InsertOne(ctx, document); err != nil {
		shared.Pulse.Logger.Error("Insert failed", err)
		return err
	}

	shared.Pulse.Logger.Debug("Insert successful", m.Set.CollectionName)
	return nil
}

func (m *MongoDBClient) FindOneAndUpdate(filter, update interface{}) (interface{}, error) {
	shared.Pulse.Logger.Debug("Finding and updating document", m.Set.CollectionName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result := m.database.Collection(m.Set.CollectionName).FindOneAndUpdate(ctx, filter, update)
	if result.Err() != nil {
		shared.Pulse.Logger.Error("FindOneAndUpdate failed", result.Err())
		return nil, result.Err()
	}

	shared.Pulse.Logger.Debugf("FindOneAndUpdate successful: %v", result.Decode(nil))
	return result, nil
}

func (m *MongoDBClient) UpdateOne(filter interface{}, update interface{}) (*mongo.UpdateResult, error) {
	shared.Pulse.Logger.Debug("Updating one document", m.Set.CollectionName)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := m.database.Collection(m.Set.CollectionName).UpdateOne(ctx, filter, update)
	if err != nil {
		shared.Pulse.Logger.Error("UpdateOne failed", err)
		return nil, err
	}
	shared.Pulse.Logger.Debugf("UpdateOne successful: %+v", result)

	return result, nil
}

// Update applies the given update to all documents matching filter in the collection.
func (m *MongoDBClient) Update(filter, update interface{}) error {
	shared.Pulse.Logger.Debug("Updating documents", m.Set.CollectionName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := m.database.Collection(m.Set.CollectionName).UpdateMany(ctx, filter, update); err != nil {
		shared.Pulse.Logger.Error("Update failed", err)
		return err
	}

	shared.Pulse.Logger.Debug("Update successful", m.Set.CollectionName)
	return nil
}

// Delete removes all documents matching filter from the collection.
func (m *MongoDBClient) Delete(filter interface{}) error {
	shared.Pulse.Logger.Debug("Deleting documents", m.Set.CollectionName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := m.database.Collection(m.Set.CollectionName).DeleteMany(ctx, filter); err != nil {
		shared.Pulse.Logger.Error("Delete failed", err)
		return err
	}

	shared.Pulse.Logger.Debug("Delete successful", m.Set.CollectionName)
	return nil
}

// Read finds and returns all documents matching filter from the collection.
func (m *MongoDBClient) Read(filter interface{}) ([]map[string]interface{}, error) {
	shared.Pulse.Logger.Debug("Reading documents", m.Set.CollectionName)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cur, err := m.database.Collection(m.Set.CollectionName).Find(ctx, filter)
	// if the collection is empty, return an empty slice

	if err != nil {
		shared.Pulse.Logger.Errorf("Read failed: %v", err)
		// If no documents exist, return empty slice
		if errors.Is(err, mongo.ErrNoDocuments) {
			shared.Pulse.Logger.Debug("No documents found", m.Set.CollectionName)
			return []map[string]interface{}{}, nil
		}
		return nil, err
	}
	defer cur.Close(ctx)

	var results []map[string]interface{}
	for cur.Next(ctx) {
		var doc map[string]interface{}
		if err := cur.Decode(&doc); err != nil {
			shared.Pulse.Logger.Error("Failed to decode document", err)
			return nil, err
		}
		if doc == nil {
			shared.Pulse.Logger.Warn("Decoded document is nil — skipping")
			continue
		}
		results = append(results, doc)
	}

	if err := cur.Err(); err != nil {
		shared.Pulse.Logger.Error("Cursor error during read", err)
		return nil, err
	}

	if len(results) == 0 {
		shared.Pulse.Logger.Warn("Collection is empty", m.Set.CollectionName)
	} else {
		shared.Pulse.Logger.Debug("Read completed", fmt.Sprintf("count=%d", len(results)))
	}

	return results, nil
}

// ReadTyped finds and returns all documents matching filter from the collection,
func ReadTyped[T any](m *MongoDBClient, filter interface{}) ([]T, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shared.Pulse.Logger.Debug("Reading typed documents", m.Set.CollectionName)

	if m.database == nil {
		return nil, fmt.Errorf("database not selected")
	}
	if m.Set.CollectionName == "" {
		return nil, fmt.Errorf("collection name not set")
	}
	if filter == nil {
		filter = bson.D{}
	}

	cur, err := m.database.Collection(m.Set.CollectionName).Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var results []T
	if err := cur.All(ctx, &results); err != nil {
		return nil, err
	}

	shared.Pulse.Logger.Debugf("Found %d documents in collection %s", len(results), m.Set.CollectionName)
	return results, nil
}
