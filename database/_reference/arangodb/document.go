package arangodb

import (
	"context"
	"fmt"
	"time"

	"github.com/arangodb/go-driver/v2/arangodb"
	"github.com/machanirobotics/loom/go/arangodb/options"
	"github.com/machanirobotics/loom/go/arangodb/shared"
	"github.com/machanirobotics/loom/go/ulid"
)

// DocumentManager defines methods for CRUD (Create, Read, Update, Delete)
// operations on documents within a *specific* collection.
type DocumentManager interface {
	// CreateDocument creates a new document in the collection. A unique `_key` is
	// automatically generated. The input document must be a map[string]interface{}.
	CreateDocument(document interface{}) (*options.Document, error)

	// ReadDocument retrieves a single document from the collection by its key.
	// Returns an error if the document is not found.
	ReadDocument(key string) (*options.Document, error)

	// UpdateDocument performs a partial update on an existing document.
	// The patch argument should be a map containing only the fields to add or change.
	UpdateDocument(key string, patch interface{}) (*options.Document, error)

	// ReplaceDocument fully replaces the data of an existing document.
	// The document argument should be a map containing the new data.
	ReplaceDocument(key string, document interface{}) (*options.Document, error)

	// DeleteDocument removes a document from the collection by its key.
	// Returns an error if the document cannot be deleted or does not exist.
	DeleteDocument(key string) error

	// ListDocuments retrieves all documents from the collection.
	ListDocuments() ([]*options.Document, error)
}

// CreateDocument adds a `_key` with a unique ULID and creates the document.
func (m *arangoInterfaceWrapper) CreateDocument(document interface{}) (*options.Document, error) {
	shared.Pulse.Logger.Debug("Entering CreateDocument", document)
	if m.Collection == nil {
		return nil, fmt.Errorf("collection is not set on the document manager")
	}
	if document == nil {
		return nil, fmt.Errorf("document cannot be nil")
	}

	docMap, ok := document.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("document must be of type map[string]interface{}")
	}

	finalDoc := make(map[string]interface{}, len(docMap)+1)
	for k, v := range docMap {
		finalDoc[k] = v
	}

	finalDoc["_key"] = ulid.GenerateString()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	meta, err := m.Collection.CreateDocument(ctx, finalDoc)
	if err != nil {
		// CORRECTED LOGGER CALL
		shared.Pulse.Logger.Error("CreateDocument failed", err)
		return nil, fmt.Errorf("failed to create document: %w", err)
	}

	shared.Pulse.Logger.Debug("CreateDocument successful", meta.Key)
	// CORRECTED RETURN: The driver's meta object does not contain the new document data.
	// We return the document we just sent to the database.
	return &options.Document{Key: meta.Key, Meta: meta.DocumentMeta, Data: finalDoc}, nil
}

// ReadDocument retrieves a single document by its key.
func (m *arangoInterfaceWrapper) ReadDocument(key string) (*options.Document, error) {
	shared.Pulse.Logger.Debug("Entering ReadDocument", key)
	if m.Collection == nil {
		return nil, fmt.Errorf("collection is not set on the document manager")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var documentData map[string]interface{}
	meta, err := m.Collection.ReadDocument(ctx, key, &documentData)
	if err != nil {
		// CORRECTED LOGGER CALL
		shared.Pulse.Logger.Error("ReadDocument failed", fmt.Sprintf("key: %s, error: %v", key, err))
		return nil, err
	}

	shared.Pulse.Logger.Debug("ReadDocument successful", key)
	return &options.Document{
		Key:  meta.Key,
		Meta: meta,
		Data: documentData,
	}, nil
}

// UpdateDocument partially updates an existing document.
func (m *arangoInterfaceWrapper) UpdateDocument(key string, patch interface{}) (*options.Document, error) {
	shared.Pulse.Logger.Debug("Entering UpdateDocument", key)
	if m.Collection == nil {
		return nil, fmt.Errorf("collection is not set on the document manager")
	}
	if patch == nil {
		return nil, fmt.Errorf("patch data cannot be nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := m.Collection.UpdateDocument(ctx, key, patch); err != nil {
		// CORRECTED LOGGER CALL
		shared.Pulse.Logger.Error("UpdateDocument failed", fmt.Sprintf("key: %s, error: %v", key, err))
		return nil, fmt.Errorf("failed to update document with key '%s': %w", key, err)
	}

	shared.Pulse.Logger.Debug("Update successful, re-reading document", key)
	return m.ReadDocument(key)
}

// ReplaceDocument fully replaces an existing document's data.
func (m *arangoInterfaceWrapper) ReplaceDocument(key string, document interface{}) (*options.Document, error) {
	shared.Pulse.Logger.Debug("Entering ReplaceDocument", key)
	if m.Collection == nil {
		return nil, fmt.Errorf("collection is not set on the document manager")
	}
	if document == nil {
		return nil, fmt.Errorf("document cannot be nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := m.Collection.ReplaceDocument(ctx, key, document); err != nil {
		// CORRECTED LOGGER CALL
		shared.Pulse.Logger.Error("ReplaceDocument failed", fmt.Sprintf("key: %s, error: %v", key, err))
		return nil, fmt.Errorf("failed to replace document with key '%s': %w", key, err)
	}

	shared.Pulse.Logger.Debug("Replace successful, re-reading document", key)
	return m.ReadDocument(key)
}

// DeleteDocument removes a document from the collection by its key.
func (m *arangoInterfaceWrapper) DeleteDocument(key string) error {
	shared.Pulse.Logger.Debug("Entering DeleteDocument", key)
	if m.Collection == nil {
		return fmt.Errorf("collection is not set on the document manager")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := m.Collection.DeleteDocument(ctx, key); err != nil {
		// CORRECTED LOGGER CALL
		shared.Pulse.Logger.Error("DeleteDocument failed", fmt.Sprintf("key: %s, error: %v", key, err))
		return fmt.Errorf("failed to delete document with key '%s': %w", key, err)
	}

	shared.Pulse.Logger.Debug("DeleteDocument successful", key)
	return nil
}

// ListDocuments retrieves all documents from the configured collection.
func (m *arangoInterfaceWrapper) ListDocuments() ([]*options.Document, error) {
	shared.Pulse.Logger.Debug("Entering ListDocuments", "")
	if m.Collection == nil {
		return nil, fmt.Errorf("collection is not set on the document manager")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := "FOR doc IN @@collection RETURN doc"
	bindVars := map[string]interface{}{
		"@collection": m.Collection.Name(),
	}

	cursor, err := m.Query(ctx, query, &arangodb.QueryOptions{
		BindVars: bindVars,
	})
	if err != nil {
		// CORRECTED LOGGER CALL
		shared.Pulse.Logger.Error("ListDocuments query failed", err)
		return nil, fmt.Errorf("AQL query failed: %w", err)
	}
	defer cursor.Close()

	var results []*options.Document
	for cursor.HasMore() {

		var docData map[string]interface{}
		meta, err := cursor.ReadDocument(ctx, &docData)
		if err != nil {
			// CORRECTED LOGGER CALL
			shared.Pulse.Logger.Error("Failed to read document from cursor", err)
			return nil, fmt.Errorf("failed to read document from query cursor: %w", err)
		}

		results = append(results, &options.Document{
			Key:  meta.Key,
			Meta: meta,
			Data: docData,
		})
	}

	shared.Pulse.Logger.Debug("ListDocuments successful", len(results))
	return results, nil
}
