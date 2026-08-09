package arangodb

import (
	"os"
	"testing"

	"github.com/arangodb/go-driver/v2/arangodb"
	_ "github.com/joho/godotenv/autoload"
	"github.com/machanirobotics/loom/go/arangodb/options"
)

func TestCollectionLifecycle(t *testing.T) {
	// --- ARRANGE: Top-level setup for all sub-tests ---
	dbName := "test_db_for_collection_lifecycle"
	collectionName := "test_people"

	endpoint := os.Getenv("ARANGODB_ENDPOINT")
	if endpoint == "" {
		t.Skip("ARANGODB_ENDPOINT environment variable not set, skipping test.")
	}

	client, err := NewArangoDBClient(options.ArangoDBClientOptions{
		Endpoints: []string{endpoint},
	})
	if err != nil {
		t.Fatalf("Failed to create ArangoDB client: %v", err)
	}

	if _, err := client.CreateDatabase(dbName); err != nil {
		t.Fatalf("Failed to create temporary database '%s': %v", dbName, err)
	}

	t.Cleanup(func() {
		t.Logf("Cleaning up: deleting database '%s'", dbName)
		if err := client.DeleteDatabase(dbName); err != nil {
			t.Errorf("Failed to delete database during cleanup: %v", err)
		}
	})

	manager, err := client.SetDatabase(dbName)
	if err != nil {
		t.Fatalf("Failed to set active database: %v", err)
	}

	// --- DEFINE SCHEMAS ---
	initialSchema := options.CollectionSchemaOptions{
		Schema: map[string]interface{}{
			"properties": map[string]interface{}{
				"name": map[string]interface{}{"type": "string"},
				"age":  map[string]interface{}{"type": "integer", "maximum": 120},
			},
			"required": []string{"name"},
		},
		Level:       options.CollectionSchemaLevelStrict,
		Description: "Initial version of the people collection.",
	}

	// --- FIX 1: Define the *complete* updated schema ---
	// It should contain all properties, not just the changed ones.
	updatedSchema := options.CollectionSchemaOptions{
		Schema: map[string]interface{}{
			"properties": map[string]interface{}{
				"name": map[string]interface{}{"type": "string"},                  // Keep original property
				"age":  map[string]interface{}{"type": "integer", "maximum": 150}, // Update maximum age
			},
			"required": []string{"name", "age"}, // Make age required
		},
		Level:       options.CollectionSchemaLevelModerate,
		Description: "Updated version of the people collection.",
	}

	// --- RUN SUB-TESTS ---

	t.Run("Create and Verify Collection", func(t *testing.T) {
		createdProps, err := manager.Collection.CreateCollection(collectionName, options.CollectionTypeDocument, initialSchema)
		if err != nil {
			t.Fatalf("CreateCollection failed: %v", err)
		}
		if createdProps.Name != collectionName {
			t.Errorf("Expected collection name to be '%s', but got '%s'", collectionName, createdProps.Name)
		}
		if createdProps.Schema.Message != initialSchema.Description {
			t.Errorf("Expected schema description to be '%s', but got '%s'", initialSchema.Description, createdProps.Schema.Message)
		}
		t.Logf("Successfully created and verified collection '%s'", collectionName)
	})

	t.Run("Get and List Collections", func(t *testing.T) {
		props, err := manager.Collection.GetCollection(collectionName)
		if err != nil {
			t.Fatalf("GetCollection failed: %v", err)
		}
		if props.Name != collectionName {
			t.Errorf("Expected GetCollection to return name '%s', but got '%s'", collectionName, props.Name)
		}

		allCollections, err := manager.Collection.ListCollections()
		if err != nil {
			t.Fatalf("ListCollections failed: %v", err)
		}
		if len(allCollections) != 1 {
			t.Fatalf("Expected 1 collection, but found %d", len(allCollections))
		}
		t.Log("Successfully verified GetCollection and ListCollections.")
	})

	t.Run("Update and Verify Collection", func(t *testing.T) {
		_, err := manager.Collection.UpdateCollection(collectionName, updatedSchema)
		if err != nil {
			t.Fatalf("UpdateCollection failed: %v", err)
		}

		propsAfterUpdate, err := manager.Collection.GetCollection(collectionName)
		if err != nil {
			t.Fatalf("GetCollection after update failed: %v", err)
		}

		// --- FIX 2: Use specific, robust assertions instead of DeepEqual ---
		if propsAfterUpdate.Schema.Message != updatedSchema.Description {
			t.Errorf("Expected persisted description to be '%s', but got '%s'", updatedSchema.Description, propsAfterUpdate.Schema.Message)
		}
		if propsAfterUpdate.Schema.Level != arangodb.CollectionSchemaLevel(updatedSchema.Level) {
			t.Errorf("Expected schema level to be '%s', but got '%s'", updatedSchema.Level, propsAfterUpdate.Schema.Level)
		}

		// Type-safe check of the schema rule
		schemaRule, ok := propsAfterUpdate.Schema.Rule.(map[string]interface{})
		if !ok {
			t.Fatalf("Schema.Rule is not a map[string]interface{} as expected")
		}
		properties, ok := schemaRule["properties"].(map[string]interface{})
		if !ok {
			t.Fatalf("'properties' field is not a map as expected")
		}
		ageProp, ok := properties["age"].(map[string]interface{})
		if !ok {
			t.Fatalf("'age' property is not a map as expected")
		}

		// Check the updated value, comparing it as a float64
		if maximum, ok := ageProp["maximum"].(float64); !ok || maximum != 150 {
			t.Errorf("Expected 'age.maximum' to be updated to 150, but got %v", ageProp["maximum"])
		}

		t.Logf("Successfully updated and verified collection '%s'", collectionName)
	})

	t.Run("Delete and Verify Deletion", func(t *testing.T) {
		if err := manager.Collection.DeleteCollection(collectionName); err != nil {
			t.Fatalf("DeleteCollection failed: %v", err)
		}
		allCollections, err := manager.Collection.ListCollections()
		if err != nil {
			t.Fatalf("ListCollections after delete failed: %v", err)
		}
		if len(allCollections) != 0 {
			t.Errorf("Expected 0 collections after deletion, but found %d", len(allCollections))
		}
		t.Logf("Successfully deleted and verified deletion of collection '%s'", collectionName)
	})
}
