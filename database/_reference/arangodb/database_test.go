package arangodb

import (
	"os"
	"testing"

	// Autoloads .env file for local testing
	_ "github.com/joho/godotenv/autoload"
	"github.com/machanirobotics/loom/go/arangodb/options"
)

// TestDatabaseLifecycle performs a full create-verify-delete-verify cycle
// for an ArangoDB database using organized sub-tests.
func TestDatabaseLifecycle(t *testing.T) {
	// --- ARRANGE: Shared setup for all sub-tests ---
	dbName := "test_db_lifecycle"

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

	// This cleanup runs after all sub-tests are complete, ensuring the
	// database is removed even if a step fails.
	t.Cleanup(func() {
		t.Logf("Cleanup: Ensuring database '%s' is deleted.", dbName)
		_ = client.DeleteDatabase(dbName)
	})

	// --- ACT & ASSERT: Run tests in sequence ---

	t.Run("Create and Verify Database", func(t *testing.T) {
		t.Logf("Attempting to create database: %s", dbName)
		_, err := client.CreateDatabase(dbName)
		if err != nil {
			t.Fatalf("Failed to create database '%s': %v", dbName, err)
		}

		// Verify the database now exists.
		_, err = client.GetDatabase(dbName)
		if err != nil {
			t.Fatalf("Failed to get database '%s' right after creation: %v", dbName, err)
		}
		t.Logf("Successfully created and verified database '%s'", dbName)
	})

	t.Run("Delete and Verify Deletion", func(t *testing.T) {
		t.Logf("Attempting to delete database: %s", dbName)
		if err := client.DeleteDatabase(dbName); err != nil {
			t.Fatalf("Failed to delete database '%s': %v", dbName, err)
		}

		// Verify the database is now gone by listing all databases.
		databases, err := client.ListDatabases()
		if err != nil {
			t.Fatalf("Failed to list databases to verify deletion: %v", err)
		}

		for _, db := range databases {
			if dbName == db.Name {
				t.Errorf("Verification failed: Database '%s' was found after it should have been deleted.", dbName)
			}
		}
		t.Logf("Successfully deleted and verified database '%s'", dbName)
	})
}
