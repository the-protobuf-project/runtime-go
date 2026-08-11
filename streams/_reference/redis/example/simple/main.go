package main

import (
	"log"

	_ "github.com/joho/godotenv/autoload"
	"github.com/machanirobotics/loom/go/redis"
)

type data struct {
	Name string `json:"name"` // Name of the person
	Age  int    `json:"age"`  // Age of the person
}

func main() {
	// Database configuration
	dbName := "bobthebuilder3"

	// Initialize a new Redis client
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

	// --- CREATE DOCUMENT ---
	log.Println("1. Creating a new document...")

	doc := redis.Document{
		Data: data{
			Name: "initialKV",
			Age:  25,
		},
	}

	createdDoc, err := manager.Document.KV.Create(doc)
	if err != nil {
		log.Fatalf("Error creating document: %s", err)
	}
	log.Printf("Created document with ID: %s\n", createdDoc.ID())

	// --- RETRIEVE DOCUMENT ---
	log.Println("\n2. Retrieving the created document...")

	gotDoc, err := manager.Document.KV.Get(createdDoc.ID())
	if err != nil {
		log.Fatalf("Error getting document: %s", err)
	}
	log.Printf("Retrieved document: %+v\n", gotDoc)

	// --- UPDATE DOCUMENT ---
	log.Println("\n3. Updating the document...")

	updateData := data{
		Name: "updatedKV",
		Age:  30,
	}
	err = manager.Document.KV.Update(createdDoc.ID(), redis.Document{
		Data: updateData,
	})
	if err != nil {
		log.Fatalf("Error updating document: %s", err)
	}
	log.Println("Document updated successfully")

	// Verify the update
	updatedDoc, err := manager.Document.KV.Get(createdDoc.ID())
	if err != nil {
		log.Fatalf("Error fetching updated document: %s", err)
	}
	log.Printf("Updated document content: %+v\n", updatedDoc)

	// --- LIST DOCUMENTS ---
	log.Println("\n4. Listing all documents...")

	docList, err := manager.Document.KV.List()
	if err != nil {
		log.Fatalf("Error listing documents: %s", err)
	}
	log.Printf("Found %d document(s):", len(docList))
	for i, doc := range docList {
		log.Printf("   %d. %+v", i+1, doc)
	}

	// --- DELETE DOCUMENT ---
	log.Printf("\n5. Deleting document %s...\n", createdDoc.ID())

	err = manager.Document.KV.Delete(createdDoc.ID())
	if err != nil {
		log.Fatalf("Error deleting document: %s", err)
	}
	log.Printf("Document deleted successfully")

	// Verify deletion
	_, err = manager.Document.KV.Get(createdDoc.ID())
	if err != nil {
		log.Printf("Verification: Document no longer exists (expected): %s\n", err)
	}

	log.Println("\nAll operations completed successfully!")
}
