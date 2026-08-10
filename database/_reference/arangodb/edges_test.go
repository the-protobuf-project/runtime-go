package arangodb

import (
	"os"
	"testing"

	_ "github.com/joho/godotenv/autoload"
	"github.com/machanirobotics/loom/go/arangodb/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testDbName         = "loom_test_db_edge"
	testNodeCollection = "test_people"
	testEdgeCollection = "test_friends"
	testGraphName      = "test_social_graph"
)

// TestEdgeManagerLifecycle covers the full CRUD (Create, Read, Update, Delete)
// lifecycle for edge documents to ensure the EdgeManager is working correctly.
func TestEdgeManagerLifecycle(t *testing.T) {
	// --- Test Setup ---
	// Skip test if ARANGODB_ENDPOINT is not set
	if os.Getenv("ARANGODB_ENDPOINT") == "" {
		t.Skip("Skipping test: ARANGODB_ENDPOINT environment variable not set.")
	}

	// Create a new client
	client, err := NewArangoDBClient(options.ArangoDBClientOptions{
		Endpoints: []string{os.Getenv("ARANGODB_ENDPOINT")},
	})
	require.NoError(t, err, "Failed to create ArangoDB client")

	// Ensure the test database is cleaned up after the test runs
	defer client.DeleteDatabase(testDbName)

	// Create a fresh database for the test
	_, err = client.CreateDatabase(testDbName)
	require.NoError(t, err, "Failed to create test database")

	// Get a manager for the new database
	manager, err := client.SetDatabase(testDbName)
	require.NoError(t, err, "Failed to set database manager")

	// ** CORRECTED CODE HERE **
	// Create a schema option that disables validation but provides a valid empty rule object.
	schemaOpts := options.CollectionSchemaOptions{
		Level:  options.CollectionSchemaLevelNone, // Disable validation
		Schema: map[string]interface{}{},          // Provide an empty, non-null object to satisfy the API
	}

	// Create the node and edge collections using the corrected 'schemaOpts'
	_, err = manager.Collection.CreateCollection(testNodeCollection, options.CollectionTypeDocument, schemaOpts)
	require.NoError(t, err, "Failed to create node collection")

	_, err = manager.Collection.CreateCollection(testEdgeCollection, options.CollectionTypeEdge, schemaOpts)
	require.NoError(t, err, "Failed to create edge collection")

	// Create a graph definition
	graphDefinition := Graph{
		Name: testGraphName,
		Edges: []Edge{
			{
				Name: testEdgeCollection, // The logical name of the edge relation
				Definition: []EdgeDefinition{
					{
						Collection: testEdgeCollection, // The physical collection name
						From:       []string{testNodeCollection},
						To:         []string{testNodeCollection},
					},
				},
			},
		},
	}
	graph, err := manager.Graph.CreateGraph(graphDefinition)
	require.NoError(t, err, "Failed to create graph")

	// Create two "node" documents to connect
	docManager, err := manager.Collection.SetCollection(testNodeCollection)
	require.NoError(t, err)

	node1, err := docManager.Document.CreateDocument(map[string]interface{}{"name": "User One"})
	require.NoError(t, err)
	node2, err := docManager.Document.CreateDocument(map[string]interface{}{"name": "User Two"})
	require.NoError(t, err)

	edgeManager := graph.Edges[0].Manager
	var createdEdge *EdgeDocumentType

	// --- Lifecycle Tests ---

	t.Run("Create Edge", func(t *testing.T) {
		edgeDoc := EdgeDocument(
			testEdgeCollection,
			string(node1.Meta.ID),
			string(node2.Meta.ID),
			map[string]interface{}{"friendship_level": "close"},
		)

		created, err := edgeManager.ConnectNode(edgeDoc)
		require.NoError(t, err)

		assert.NotEmpty(t, created.ID, "Created edge should have an ID from the database")
		assert.Equal(t, string(node1.Meta.ID), created.From)
		assert.Equal(t, string(node2.Meta.ID), created.To)
		assert.Equal(t, "close", created.Metadata["friendship_level"])

		// Store the created edge for subsequent tests
		createdEdge = created
	})

	t.Run("Get Edge", func(t *testing.T) {
		require.NotNil(t, createdEdge, "Cannot run Get test, edge creation failed")

		retrievedEdge, err := edgeManager.GetEdge(createdEdge.ID, testEdgeCollection)
		require.NoError(t, err)

		assert.Equal(t, createdEdge.ID, retrievedEdge.ID)
		assert.Equal(t, createdEdge.From, retrievedEdge.From)
		assert.Equal(t, createdEdge.Metadata["friendship_level"], retrievedEdge.Metadata["friendship_level"])
	})

	t.Run("List Edges", func(t *testing.T) {
		edges, err := edgeManager.ListEdges(testEdgeCollection)
		require.NoError(t, err)

		assert.Len(t, edges, 1, "Should be exactly one edge in the collection")
		assert.Equal(t, createdEdge.ID, edges[0].ID)
	})

	t.Run("Update Edge", func(t *testing.T) {
		require.NotNil(t, createdEdge, "Cannot run Update test, edge creation failed")

		updatePayload := EdgeDocumentType{
			CollectionName: testEdgeCollection,
			From:           createdEdge.From,
			To:             createdEdge.To,
			Metadata:       map[string]interface{}{"friendship_level": "best"},
		}

		updatedEdge, err := edgeManager.UpdateEdge(createdEdge.ID, updatePayload)
		require.NoError(t, err)

		assert.Equal(t, createdEdge.ID, updatedEdge.ID, "ID should not change on update")
		assert.Equal(t, "best", updatedEdge.Metadata["friendship_level"], "Metadata should be updated")

		// Store the updated edge for the delete test
		createdEdge = updatedEdge
	})

	t.Run("Delete Edge", func(t *testing.T) {
		require.NotNil(t, createdEdge, "Cannot run Delete test, prior steps failed")

		err := edgeManager.DisconnectNode(createdEdge)
		require.NoError(t, err)

		// Verification: Try to get the edge again, it should fail
		_, err = edgeManager.GetEdge(createdEdge.ID, testEdgeCollection)
		assert.Error(t, err, "Expected an error when trying to get a deleted document")
	})
}
