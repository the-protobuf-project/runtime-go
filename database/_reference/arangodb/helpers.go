package arangodb

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/arangodb/go-driver/v2/arangodb"
	arngshd "github.com/arangodb/go-driver/v2/arangodb/shared"
	"github.com/machanirobotics/loom/go/arangodb/options"
	"github.com/machanirobotics/loom/go/arangodb/shared"
	"github.com/machanirobotics/loom/go/ulid"
)

// convertToEdgeName converts a given edge name into UPPER_SNAKE_CASE and appends a "_EDGE" suffix.
// It handles camelCase, PascalCase, snake_case, dash-case, and space-separated strings.
//
// Examples:
//
//	convertToEdgeName("notSoCute")        // "NOT_SO_CUTE_EDGE"
//	convertToEdgeName("already_clean")    // "ALREADY_CLEAN_EDGE"
//	convertToEdgeName("PascalCaseWord")   // "PASCAL_CASE_WORD_EDGE"
func convertToEdgeName(name string) string {
	// Step 1: Normalize delimiters by converting "-" and "_" to spaces
	name = strings.ReplaceAll(name, "-", " ")
	name = strings.ReplaceAll(name, "_", " ")
	name = strings.TrimSpace(name)

	// Step 2: Insert space between camelCase or PascalCase transitions
	re := regexp.MustCompile(`([a-z0-9])([A-Z])`)
	name = re.ReplaceAllString(name, "${1} ${2}")

	// Step 3: Replace multiple spaces with a single space
	name = regexp.MustCompile(`\s+`).ReplaceAllString(name, " ")

	// Step 4: Split into words, join with underscores, and convert to uppercase
	parts := strings.Fields(name)
	processedName := strings.ToUpper(strings.Join(parts, "_"))

	// --- FIX: Conditionally add the "_EDGE" suffix ---
	// Step 5: Check if the name already ends with "_EDGE" before adding it.
	if !strings.HasSuffix(processedName, "_EDGE") {
		return processedName + "_EDGE"
	}

	// If the suffix already exists, return the processed name as is.
	return processedName
}

// EdgeDocumentEntry defines the structure of an edge document in ArangoDB.
type EdgeDocumentType struct {
	ID             string `json:"_key"` // Unique identifier for the edge document
	CollectionName string
	From           string                 `json:"_from"`              // Document ID of the source vertex
	To             string                 `json:"_to"`                // Document ID of the target vertex
	Metadata       map[string]interface{} `json:"metadata,omitempty"` // Optional metadata for the edge
}

// EdgeDocument creates a new edge document with a generated unique ID, and provided
// `_from`, `_to`, and optional metadata.
//
// Example:
//
//	doc := EdgeDocument("people/123", "people/456", map[string]interface{}{"weight": 0.9})
func EdgeDocument(CollectionName, From, To string, Metadata map[string]interface{}) EdgeDocumentType {
	data := EdgeDocumentType{
		ID:             ulid.GenerateString(),
		CollectionName: CollectionName,
		From:           From,
		To:             To,
		Metadata:       Metadata,
	}
	shared.Pulse.Logger.Debug("Creating edge document", data)
	return data
}

// ToMapOfStringInterface converts the EdgeDocumentType to a map[string]interface{}.
// This is useful for creating the document in ArangoDB.
func (e *EdgeDocumentType) ToMapOfStringInterface() map[string]interface{} {
	docMap := make(map[string]interface{})
	docMap["_key"] = e.ID
	docMap["_from"] = e.From
	docMap["_to"] = e.To
	docMap["metadata"] = e.Metadata
	return docMap
}

// toGraphObject converts an ArangoDB graph to an internal Graph representation
// by mapping its name and edges.
func (g *arangoInterfaceWrapper) toGraphObject(graph arangodb.Graph, input Graph) *Graph {
	graphObj := &Graph{
		Name:  graph.Name(),
		Edges: make([]Edge, len(input.Edges)),
	}
	for i, edge := range input.Edges {
		graphObj.Edges[i] = Edge{
			Name:       edge.Name,
			Definition: edge.Definition,
			Manager:    g,
		}
	}
	return graphObj
}

// toEdgeObject converts a list of ArangoDB EdgeDefinition objects into internal Edge objects.
func (g *arangoInterfaceWrapper) toEdgeObject(defs []arangodb.EdgeDefinition) []Edge {
	var edges []Edge
	for _, def := range defs {
		edges = append(edges, Edge{
			Name: def.Collection,
			Definition: []EdgeDefinition{{ // Adjust if multiple definitions are supported
				Collection: def.Collection,
				From:       def.From,
				To:         def.To,
			}},
			Manager: g,
		})
	}
	return edges
}

// checkIfCollectionExistsAndCreateEdge checks if a given edge collection exists in the database,
// and creates it if it does not. Collection names are normalized using convertToEdgeName.
// If the collection already exists, it is returned without error.
func (g *arangoInterfaceWrapper) checkIfCollectionExistsAndCreateEdge(ctx context.Context, collName string) (arangodb.Collection, error) {
	convertedName := convertToEdgeName(collName)

	collection, err := g.Database.GetCollection(ctx, convertedName, nil)
	if err == nil {
		return collection, nil
	}

	collectionType := arangodb.CollectionTypeEdge

	collection, err = g.CreateCollectionV2(ctx, convertedName, &arangodb.CreateCollectionPropertiesV2{
		Type: &collectionType,
	})
	if err != nil {
		if arngshd.IsArangoErrorWithErrorNum(err,
			arngshd.ErrArangoConflict,
			arngshd.ErrArangoIllegalName,
			arngshd.ErrArangoUniqueConstraintViolated,
			arngshd.ErrUserDuplicate,
		) {
			shared.Pulse.Logger.Warnf("Collection '%s' already exists, skipping creation", convertedName)
			return g.Database.GetCollection(ctx, convertedName, nil)
		}
		return nil, fmt.Errorf("failed to create edge collection '%s': %w", convertedName, err)
	}

	return collection, nil
}

// parseEdgeDefinitions converts internal Edge definitions into ArangoDB-compatible EdgeDefinition structs.
// Each edge name is normalized via convertToEdgeName before being used as a collection name.
func (g *arangoInterfaceWrapper) parseEdgeDefinitions(edges []Edge) []arangodb.EdgeDefinition {
	var edgeDefs []arangodb.EdgeDefinition
	for _, edge := range edges {
		edgeDef := arangodb.EdgeDefinition{
			Collection: convertToEdgeName(edge.Name),
			From:       edge.Definition[0].From,
			To:         edge.Definition[0].To,
		}
		edgeDefs = append(edgeDefs, edgeDef)
	}
	return edgeDefs
}

// mapToEdgeDocument safely converts a generic driver document into an EdgeDocumentType.
func mapToEdgeDocument(doc *options.Document, collectionName string) (*EdgeDocumentType, error) {
	dataMap, ok := doc.Data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("document data for key %s is not the expected map type", doc.Key)
	}

	// Safely get the '_from' field
	from, ok := dataMap["_from"].(string)
	if !ok {
		return nil, fmt.Errorf("document %s is missing '_from' field or it's not a string", doc.Key)
	}

	// Safely get the '_to' field
	to, ok := dataMap["_to"].(string)
	if !ok {
		return nil, fmt.Errorf("document %s is missing '_to' field or it's not a string", doc.Key)
	}

	// Safely get the 'metadata' field
	metadata, ok := dataMap["metadata"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("document %s is missing 'metadata' field or it's not a map", doc.Key)
	}

	edge := &EdgeDocumentType{
		ID:             doc.Key,
		CollectionName: collectionName,
		From:           from,
		To:             to,
		Metadata:       metadata,
	}

	return edge, nil
}
