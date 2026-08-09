package main

import (
	"os"
	"time"

	"github.com/charmbracelet/log"
	_ "github.com/joho/godotenv/autoload"
	"github.com/machanirobotics/loom/go/arangodb"
	"github.com/machanirobotics/loom/go/arangodb/options"
	"github.com/machanirobotics/loom/go/arangodb/shared"
)

func main() {

	client, err := arangodb.NewArangoDBClient(options.ArangoDBClientOptions{
		Endpoints: []string{os.Getenv("ARANGODB_ENDPOINT")},
	})
	if err != nil {
		panic(err)
	}

	defer client.Close()

	client.DeleteDatabase(dbName)

	createDatabase, err := client.CreateDatabase(dbName)
	if err != nil {
		log.Errorf("Failed to create database: %v", err)
	}
	log.Infof("Created database successfully: %v", createDatabase)

	manager, _ := client.SetDatabase(dbName) // Requires a database to be set before creating collections

	// Create Collection
	createCollection, err := manager.Collection.CreateCollection("people", options.CollectionTypeDocument, options.CollectionSchemaOptions{
		Schema:      collectionSchema.Schema,
		Level:       options.CollectionSchemaLevelStrict,
		Description: "Collection for storing people data",
	})
	if err != nil {
		log.Errorf("Failed to create collection: %v", err)
	}
	log.Infof("Created collection successfully: %+v", createCollection)
	// The DocumentManager is now scoped to the 'users' collection
	docManager, err := manager.Collection.SetCollection(collectionName)
	if err != nil {
		log.Fatalf("Failed to get document manager: %v", err)
	}

	// Set the collection for further operations
	resp, err := docManager.Document.CreateDocument(map[string]interface{}{
		"name": "Bob The Builder",
		"age":  25,
	})
	if err != nil {
		log.Errorf("Failed to create document: %v", err)
	}
	log.Infof("Created document successfully: %+v", resp)

	// Set the collection for further operations
	resp2, err := docManager.Document.CreateDocument(map[string]interface{}{
		"name": "Alice The Architect",
		"age":  26,
	})
	if err != nil {
		log.Errorf("Failed to create document: %v", err)
	}
	log.Infof("Created document successfully: %+v", resp2)

	// Create a graph
	graph, err := manager.Graph.CreateGraph(arangodb.Graph{
		Name:  "social_network",
		Edges: TestingEdge,
	})
	if err != nil {
		log.Errorf("Failed to create graph: %v", err)
	} else {
		log.Infof("Created graph successfully: %v", graph)
	}

	// Get the graph
	retrievedGraph, err := manager.Graph.GetGraph("social_network")
	if err != nil {
		log.Errorf("Failed to get graph: %v", err)
	} else {
		log.Infof("Retrieved graph successfully: %v", retrievedGraph)
	}

	shared.Pulse.Logger.Debug("Connecting nodes in graph", graph.Edges[0].Name)

	document := arangodb.EdgeDocument(graph.Edges[0].Name,
		string(resp.Meta.ID),
		string(resp2.Meta.ID),
		map[string]interface{}{
			"since":            "2023-01-01",
			"friendship_level": "close",
		},
	)
	shared.Pulse.Logger.Warn("Edge Document", document)

	// Create an edge document
	data, err := graph.Edges[0].Manager.ConnectNode(document)

	if err != nil {
		log.Errorf("Failed to create edge document: %v", err)
	} else {
		log.Infof("Created edge document successfully: %+v", data)
	}

	// Get the edge document
	d, err := graph.Edges[0].Manager.GetEdge(data.ID, graph.Edges[0].Name)
	if err != nil {
		log.Errorf("Failed to get edge document: %v", err)
	} else {
		log.Infof("Retrieved edge document successfully: %+v", d)
	}

	// List all the edges in the graph
	edges, err := graph.Edges[0].Manager.ListEdges(graph.Edges[0].Name)
	if err != nil {
		log.Errorf("Failed to list edges: %v", err)
	} else {
		for _, edge := range edges {
			log.Infof("Edge: %+v", edge)
		}
	}

	time.Sleep(8 * time.Second) // Wait for a while to ensure the update is reflected

	// Update the edge document
	updatePayload := arangodb.EdgeDocumentType{
		CollectionName: graph.Edges[0].Name,
		From:           string(resp.Meta.ID),
		To:             string(resp2.Meta.ID),
		Metadata: map[string]interface{}{
			"since":            "2023-01-01",
			"friendship_level": "best",
		},
	}
	updatedData, err := graph.Edges[0].Manager.UpdateEdge(data.ID, updatePayload)
	if err != nil {
		log.Errorf("Failed to update edge document: %v", err)
	}

	time.Sleep(8 * time.Second) // Wait for a while to ensure the update is reflected

	// Print the updated edge document
	updatedEdge, err := graph.Edges[0].Manager.GetEdge(updatedData.ID, graph.Edges[0].Name)
	if err != nil {
		log.Errorf("Failed to get updated edge document: %v", err)
	} else {
		log.Warnf("Updated edge document: %+v", updatedEdge)
	}

	// // Delete the edge document
	// if err := graph.Edges[0].Manager.DisconnectNode(updatedEdge); err != nil {
	// 	log.Errorf("Failed to delete edge document: %v", err)
	// } else {
	// 	log.Warnf("Deleted edge document successfully: %s", data.ID)
	// }

}
