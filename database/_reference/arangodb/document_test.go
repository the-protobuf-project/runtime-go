package arangodb

import (
	"os"
	"testing"

	_ "github.com/joho/godotenv/autoload"
	"github.com/machanirobotics/loom/go/arangodb/options"
)

func TestDocumentLifecycle(t *testing.T) {
	// --- ARRANGE: Top-level setup for all sub-tests ---
	dbName := "test_db_for_documents"
	collectionName := "test_users"

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

	// --- FIX 1: Provide a valid, non-nil schema ---
	// The CreateCollection call was also passing an extra incorrect argument which has been removed.
	if _, err := manager.Collection.CreateCollection(collectionName, options.CollectionTypeDocument, options.CollectionSchemaOptions{
		Schema: map[string]interface{}{}, // Provide an empty map, which is a valid schema object.
	}); err != nil {
		t.Fatalf("Failed to create collection '%s': %v", collectionName, err)
	}

	// --- FIX 2: Use DocumentManager() to get a correctly scoped manager ---
	docManager, err := manager.Collection.SetCollection(collectionName)
	if err != nil {
		t.Fatalf("Failed to get document manager for collection '%s': %v", collectionName, err)
	}

	var docKey string

	// --- RUN SUB-TESTS IN SEQUENCE ---

	t.Run("Create Document", func(t *testing.T) {
		docToCreate := map[string]interface{}{"name": "John Doe", "age": 30}
		createdDoc, err := docManager.Document.CreateDocument(docToCreate)
		if err != nil {
			t.Fatalf("Failed to create document: %v", err)
		}
		if createdDoc.Key == "" {
			t.Fatal("CreateDocument returned an empty key")
		}
		docKey = createdDoc.Key
		t.Logf("Successfully created document with key: %s", docKey)
	})

	t.Run("Read and Verify Initial Document", func(t *testing.T) {
		if docKey == "" {
			t.Fatal("Document key was not set by the Create step")
		}
		readDoc, err := docManager.Document.ReadDocument(docKey)
		if err != nil {
			t.Fatalf("Failed to read document: %v", err)
		}

		dataMap, ok := readDoc.Data.(map[string]interface{})
		if !ok {
			t.Fatalf("The returned document data is not a map as expected.")
		}

		if name, _ := dataMap["name"].(string); name != "John Doe" {
			t.Errorf("Expected name to be 'John Doe', got '%s'", name)
		}
		if age, _ := dataMap["age"].(float64); age != 30 {
			t.Errorf("Expected age to be 30, got '%v'", age)
		}
	})

	t.Run("Update and Verify Document", func(t *testing.T) {
		if docKey == "" {
			t.Fatal("Document key was not set by the Create step")
		}
		patchData := map[string]interface{}{
			"age":    35,
			"status": "active",
		}

		updatedDoc, err := docManager.Document.UpdateDocument(docKey, patchData)
		if err != nil {
			t.Fatalf("Failed to update document: %v", err)
		}

		dataMap, ok := updatedDoc.Data.(map[string]interface{})
		if !ok {
			t.Fatalf("The returned document data is not a map as expected.")
		}

		if age, _ := dataMap["age"].(float64); age != 35 {
			t.Errorf("Expected updated age to be 35, got '%v'", age)
		}
		if status, _ := dataMap["status"].(string); status != "active" {
			t.Errorf("Expected status to be 'active', got '%s'", status)
		}
	})

	t.Run("Delete and Verify Deletion", func(t *testing.T) {
		if docKey == "" {
			t.Fatal("Document key was not set by the Create step")
		}
		err := docManager.Document.DeleteDocument(docKey)
		if err != nil {
			t.Fatalf("Failed to delete document: %v", err)
		}
		t.Logf("Successfully deleted document with key: %s", docKey)

		_, err = docManager.Document.ReadDocument(docKey)
		if err == nil {
			t.Fatalf("Expected error when reading deleted document, but got none")
		}
		docs, err := docManager.Document.ListDocuments()
		if err != nil {
			t.Fatalf("ListDocuments failed after deletion: %v", err)
		}
		if len(docs) != 0 {
			t.Errorf("Expected 0 documents after deletion, but ListDocuments found %d", len(docs))
		}
	})
}
