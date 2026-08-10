package arangodb

import (
	"os"
	"testing"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/machanirobotics/loom/go/arangodb/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGraphManagerLifecycle covers the full CRUD (Create, Read, Update, Delete)
// lifecycle for graph definitions to ensure the EdgeManager is working correctly.
func TestGraphManagerLifecycle(t *testing.T) {
	// --- Test Setup ---
	if os.Getenv("ARANGODB_ENDPOINT") == "" {
		t.Skip("Skipping test: ARANGODB_ENDPOINT environment variable not set.")
	}

	client, err := NewArangoDBClient(options.ArangoDBClientOptions{
		Endpoints: []string{os.Getenv("ARANGODB_ENDPOINT")},
	})
	require.NoError(t, err, "Failed to create ArangoDB client")

	const testDbName = "loom_test_db_graph"
	// Ensure the test database is cleaned up after the test runs
	defer client.DeleteDatabase(testDbName)

	_, err = client.CreateDatabase(testDbName)
	require.NoError(t, err, "Failed to create test database")

	manager, err := client.SetDatabase(testDbName)
	require.NoError(t, err, "Failed to set database manager")

	// Define constants for our test graph
	const (
		graphName      = "test_social_network"
		nodeCollection = "test_users"
		edgeCollection = "TEST_FRIENDSHIPS_EDGE"
	)

	schemaOpts := options.CollectionSchemaOptions{
		Level:  options.CollectionSchemaLevelNone, // Disable validation
		Schema: map[string]interface{}{},          // Provide an empty, non-null object to satisfy the API
	}

	_, err = manager.Collection.CreateCollection(nodeCollection, options.CollectionTypeDocument, schemaOpts)
	require.NoError(t, err, "Failed to create node collection 'test_users'")

	_, err = manager.Collection.CreateCollection(edgeCollection, options.CollectionTypeEdge, schemaOpts)
	require.NoError(t, err, "Failed to create edge collection 'test_friendships'")

	// Define the graph structure for creation
	graphDefinition := Graph{
		Name: graphName,
		Edges: []Edge{
			{
				Name: edgeCollection,
				Definition: []EdgeDefinition{
					{
						Collection: edgeCollection,
						From:       []string{nodeCollection},
						To:         []string{nodeCollection},
					},
				},
			},
		},
	}

	var createdGraph *Graph

	// --- Lifecycle Tests ---

	t.Run("Create Graph", func(t *testing.T) {
		// This will now pass because the collections exist
		graph, err := manager.Graph.CreateGraph(graphDefinition)
		require.NoError(t, err)
		require.NotNil(t, graph)

		assert.Equal(t, graphName, graph.Name)
		require.Len(t, graph.Edges, 1, "Graph should have one edge definition")
		assert.Equal(t, edgeCollection, graph.Edges[0].Name)
		assert.NotNil(t, graph.Edges[0].Manager, "Edge manager should be initialized")

		// Store the created graph for subsequent tests
		createdGraph = graph

		// Add a small delay to allow the database to achieve consistency.
		time.Sleep(500 * time.Millisecond)
	})

	t.Run("Get Graph", func(t *testing.T) {
		require.NotNil(t, createdGraph, "Cannot run Get test, graph creation failed")

		retrievedGraph, err := manager.Graph.GetGraph(graphName)
		require.NoError(t, err)
		require.NotNil(t, retrievedGraph)

		assert.Equal(t, createdGraph.Name, retrievedGraph.Name)
		require.Len(t, retrievedGraph.Edges, 1)
		assert.Equal(t, createdGraph.Edges[0].Name, retrievedGraph.Edges[0].Name)
	})

	t.Run("Fail to Create Duplicate Graph", func(t *testing.T) {
		// Attempting to create the same graph again should result in an error.
		_, err := manager.Graph.CreateGraph(graphDefinition)
		assert.Error(t, err, "Should receive an error when creating a duplicate graph")
	})

	t.Run("List Graphs", func(t *testing.T) {
		graphs, err := manager.Graph.ListGraphs()
		require.NoError(t, err)

		assert.Len(t, graphs, 1, "There should be exactly one graph in the database")
		assert.Equal(t, graphName, graphs[0].Name)
	})

	t.Run("Delete Graph", func(t *testing.T) {
		require.NotNil(t, createdGraph, "Cannot run Delete test, prior steps failed")

		err := manager.Graph.DeleteGraph(graphName)
		require.NoError(t, err)

		// Verification: Try to get the graph again; it should fail.
		_, err = manager.Graph.GetGraph(graphName)
		assert.Error(t, err, "Expected an error when trying to get a deleted graph")
	})
}
