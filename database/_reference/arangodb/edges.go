package arangodb

import (
	"fmt"

	"github.com/machanirobotics/loom/go/arangodb/shared"
)

// EdgeManager defines the standard set of operations for managing edge documents in a graph.
// It provides a consistent API for creating, retrieving, updating, and deleting edges,
// abstracting the underlying database driver implementation.
type EdgeManager interface {
	// ConnectNode creates a new edge document in a collection.
	// The `edge` parameter specifies the connection details, including `_from`, `_to`,
	// the target collection, and any associated metadata.
	// It returns a pointer to the newly created EdgeDocumentType, including its
	// system-assigned key, upon successful creation.
	ConnectNode(edge EdgeDocumentType) (*EdgeDocumentType, error)

	// DisconnectNode removes an edge between two nodes.
	// It identifies the edge to be deleted using the ID and collection name
	// stored within the provided `edge` struct.
	DisconnectNode(edge *EdgeDocumentType) error

	// GetEdge retrieves a single edge document by its unique ID from a given collection.
	// The `id` is the document's key (`_key`). The `collectionName` specifies
	// the edge collection to search within.
	// It returns the found edge document or an error if the document cannot be found or parsed.
	GetEdge(id, collectionName string) (*EdgeDocumentType, error)

	// ListEdges returns all edge documents within a specified collection.
	// This function retrieves all documents from the collection and maps them to a
	// slice of *EdgeDocumentType.
	ListEdges(collectionName string) ([]*EdgeDocumentType, error)

	// UpdateEdge updates the content of an existing edge document, identified by its `id`.
	// The `edge` parameter contains the new data that will replace the existing document's data.
	// It returns the updated edge document after a successful operation.
	UpdateEdge(id string, edge EdgeDocumentType) (*EdgeDocumentType, error)
}

// ConnectNode implements the EdgeManager interface. It creates a new edge document
// in the specified collection using the data provided in the `edge` parameter.
func (m *arangoInterfaceWrapper) ConnectNode(edge EdgeDocumentType) (*EdgeDocumentType, error) {
	convertedName := convertToEdgeName(edge.CollectionName)
	shared.Pulse.Logger.Debugf("ArangoDB client: connecting nodes in edge collection %s", convertedName)

	edgeCollection, err := m.SetCollection(convertedName)
	if err != nil {
		shared.Pulse.Logger.Error("ConnectNode failed to get edge collection", fmt.Sprintf("collection: %s, error: %v", convertedName, err))
		return nil, fmt.Errorf("failed to get edge collection '%s': %w", convertedName, err)
	}

	doc, err := edgeCollection.Document.CreateDocument(edge.ToMapOfStringInterface())
	if err != nil {
		shared.Pulse.Logger.Error("ConnectNode failed to create document", fmt.Sprintf("error: %v", err))
		return nil, fmt.Errorf("failed to connect nodes with edge '%s': %w", edge.CollectionName, err)
	}
	edge.ID = doc.Meta.Key
	shared.Pulse.Logger.Debug("ConnectNode successful", fmt.Sprintf("from: %s, to: %s", edge.From, edge.To))
	return &edge, nil
}

// DisconnectNode implements the EdgeManager interface. It removes an edge document
// from the database. The edge is identified by its ID field.
func (m *arangoInterfaceWrapper) DisconnectNode(edge *EdgeDocumentType) error {
	shared.Pulse.Logger.Debugf("ArangoDB client: disconnecting nodes in edge collection %s", edge.CollectionName)
	edgeCollection, err := m.SetCollection(edge.CollectionName)
	if err != nil {
		shared.Pulse.Logger.Error("DisconnectNode failed to get edge collection", fmt.Sprintf("collection: %s, error: %v", edge.CollectionName, err))
		return fmt.Errorf("failed to get edge collection '%s': %w", edge.CollectionName, err)
	}
	if err := edgeCollection.Document.DeleteDocument(edge.ID); err != nil {
		shared.Pulse.Logger.Error("DisconnectNode failed to delete document", fmt.Sprintf("id: %s, error: %v", edge.ID, err))
		return fmt.Errorf("failed to disconnect edge with ID '%s': %w", edge.ID, err)
	}
	shared.Pulse.Logger.Debug("DisconnectNode successful", fmt.Sprintf("edge ID: %s", edge.ID))
	return nil
}

// GetEdge implements the EdgeManager interface. It retrieves a specific edge document
// from the database by its ID and collection name.
func (m *arangoInterfaceWrapper) GetEdge(id, collectionName string) (*EdgeDocumentType, error) {
	convertedName := convertToEdgeName(collectionName)
	shared.Pulse.Logger.Debugf("ArangoDB client: getting edge document from collection %s", convertedName)

	edgeCollection, err := m.SetCollection(convertedName)
	if err != nil {
		shared.Pulse.Logger.Error("GetEdge failed to get edge collection", fmt.Sprintf("collection: %s, error: %v", convertedName, err))
		return nil, fmt.Errorf("failed to get edge collection '%s': %w", convertedName, err)
	}
	doc, err := edgeCollection.Document.ReadDocument(id)
	if err != nil {
		shared.Pulse.Logger.Error("GetEdge failed to retrieve document", fmt.Sprintf("id: %s, error: %v", id, err))
		return nil, fmt.Errorf("failed to get edge document with ID '%s': %w", id, err)
	}

	data, err := mapToEdgeDocument(doc, convertedName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse edge document with ID '%s': %w", id, err)
	}

	shared.Pulse.Logger.Debug("GetEdge successful", fmt.Sprintf("edge ID: %s", id))
	return data, nil
}

// ListEdges implements the EdgeManager interface. It retrieves all documents from the
// specified edge collection and returns them as a slice of *EdgeDocumentType.
func (m *arangoInterfaceWrapper) ListEdges(collectionName string) ([]*EdgeDocumentType, error) {
	convertedName := convertToEdgeName(collectionName)
	shared.Pulse.Logger.Debugf("ArangoDB client: listing edges in collection %s", convertedName)

	edgeCollection, err := m.SetCollection(convertedName)
	if err != nil {
		shared.Pulse.Logger.Error("ListEdges failed to get edge collection", fmt.Sprintf("collection: %s, error: %v", convertedName, err))
		return nil, fmt.Errorf("failed to get edge collection '%s': %w", convertedName, err)
	}

	docs, err := edgeCollection.Document.ListDocuments()
	if err != nil {
		shared.Pulse.Logger.Error("ListEdges failed to list documents", fmt.Sprintf("error: %v", err))
		return nil, fmt.Errorf("failed to list edges in collection '%s': %w", convertedName, err)
	}

	var edges []*EdgeDocumentType
	for _, doc := range docs {
		edge, err := mapToEdgeDocument(doc, convertedName)
		if err != nil {
			shared.Pulse.Logger.Error("ListEdges failed to map document to EdgeDocumentType", fmt.Sprintf("error: %v", err))
			continue
		}
		if edge == nil {
			shared.Pulse.Logger.Warn("ListEdges found nil edge document, skipping")
			continue
		}
		shared.Pulse.Logger.Debugf("ListEdges found edge document: %+v", edge)
		edges = append(edges, edge)
	}
	shared.Pulse.Logger.Debugf("ListEdges successful, found %d edges", len(edges))
	return edges, nil
}

// UpdateEdge implements the EdgeManager interface. It finds an edge by its ID and
// replaces its data with the content of the provided `edge` struct.
func (m *arangoInterfaceWrapper) UpdateEdge(id string, edge EdgeDocumentType) (*EdgeDocumentType, error) {
	convertedName := convertToEdgeName(edge.CollectionName)
	shared.Pulse.Logger.Debugf("ArangoDB client: updating edge document in collection %s", convertedName)

	edgeCollection, err := m.SetCollection(convertedName)
	if err != nil {
		shared.Pulse.Logger.Error("UpdateEdge failed to get edge collection", fmt.Sprintf("collection: %s, error: %v", convertedName, err))
		return nil, fmt.Errorf("failed to get edge collection '%s': %w", convertedName, err)
	}

	docMap := edge.ToMapOfStringInterface()
	doc, err := edgeCollection.Document.ReplaceDocument(id, docMap)
	if err != nil {
		shared.Pulse.Logger.Error("UpdateEdge failed to update document", fmt.Sprintf("id: %s, error: %v", id, err))
		return nil, fmt.Errorf("failed to update edge document with ID '%s': %w", id, err)
	}
	edgeData, err := mapToEdgeDocument(doc, convertedName)
	if err != nil {
		shared.Pulse.Logger.Error("ListEdges failed to map document to EdgeDocumentType", fmt.Sprintf("error: %v", err))
		return nil, fmt.Errorf("failed to parse updated edge document with ID '%s': %w", id, err)
	}
	shared.Pulse.Logger.Debug("UpdateEdge successful", fmt.Sprintf("edge ID: %s", id))
	return edgeData, nil
}
