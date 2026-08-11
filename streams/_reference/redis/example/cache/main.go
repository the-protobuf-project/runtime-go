package main

import (
	"log"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/machanirobotics/loom/go/google/resourcename"
	"github.com/machanirobotics/loom/go/redis"
)

type data struct {
	_    struct{} `resource:"//machanirobotics.com/user/{name}"`
	Name string   `json:"name" resource:"name"`
	Age  int      `json:"age"`
}

func main() {
	// Database configuration
	dbName := "cache_example_db"

	dbManager, err := redis.NewRedisClient()
	if err != nil {
		log.Fatalf("Failed to create Redis client: %v", err)
	}

	// Create a new database (if it doesn't exist)
	if err := dbManager.CreateDatabase(dbName); err != nil {
		log.Printf("Database creation note: %v", err)
	}

	// Set the active database
	manager, err := dbManager.SetDatabase(dbName)
	if err != nil {
		log.Fatalf("Failed to set database: %v", err)
	}

	// --- CREATE CACHED DOCUMENT ---
	log.Println("1. Creating a new cached document...")

	userData := data{
		Name: "John Doe",
		Age:  25,
	}

	// Create a document with a 10-second TTL (Time To Live)
	doc := redis.Document{
		Data: userData,
		TTL:  10 * time.Second,
	}

	// Use the resource name as the cache key/index ID.
	cacheID, err := resourcename.MarshalResource(userData)
	if err != nil {
		log.Fatalf("Error building resource name: %s", err)
	}
	doc.SetID(cacheID)

	createdDoc, err := manager.Document.Cache.Create(doc)
	if err != nil {
		log.Fatalf("Error creating cached document: %s", err)
	}
	log.Printf("Created cached document with ID: %s (TTL: %v seconds)\n",
		createdDoc.ID(), doc.TTL)

	// --- RETRIEVE CACHED DOCUMENT ---
	log.Println("\n2. Retrieving the cached document...")

	gotDoc, err := manager.Document.Cache.Get(createdDoc.ID())
	if err != nil {
		log.Fatalf("Error getting cached document: %s", err)
	}
	log.Printf("Retrieved cached document: %+v\n", gotDoc)

	var parsedFromResource data
	if err := resourcename.UnmarshalResource(gotDoc.ID(), &parsedFromResource); err != nil {
		log.Fatalf("Error unmarshaling resource name %q: %v", gotDoc.ID(), err)
	}
	log.Printf("Unmarshaled resource name -> %+v\n", parsedFromResource)

	// --- UPDATE CACHED DOCUMENT ---
	log.Println("\n3. Updating the cached document...")

	updateData := data{
		Name: "updatedCacheItem",
		Age:  30,
	}

	err = manager.Document.Cache.Update(createdDoc.ID(), redis.Document{
		Data: updateData,
		TTL:  20 * time.Second,
	})
	if err != nil {
		log.Fatalf("Error updating cached document: %s", err)
	}
	log.Println("Cached document updated successfully")

	// Verify the update
	updatedDoc, err := manager.Document.Cache.Get(createdDoc.ID())
	if err != nil {
		log.Fatalf("Error fetching updated document: %s", err)
	}
	log.Printf("Updated document content: %+v\n", updatedDoc)

	// --- LIST CACHED DOCUMENTS ---
	log.Println("\n4. Listing all cached documents...")

	docList, err := manager.Document.Cache.List()
	if err != nil {
		log.Fatalf("Error listing cached documents: %s", err)
	}
	log.Printf("Found %d cached document(s):", len(docList))
	for i, doc := range docList {
		log.Printf("   %d. %+v", i+1, doc)
	}

	// --- CLEANUP ---
	log.Println("\n5. Cleaning up...")

	// Delete the original document
	err = manager.Document.Cache.Delete(createdDoc.ID())
	if err != nil {
		log.Printf("  - Error deleting document: %s", err)
	} else {
		log.Printf("  - Deleted document: %s", createdDoc.ID())
	}

	// Verify deletion
	if _, err := manager.Document.Cache.Get(createdDoc.ID()); err != nil {
		log.Printf("  - Document %s no longer exists (expected)\n", createdDoc.ID())
	}

	log.Println("\nCache demonstration completed successfully!")
}
