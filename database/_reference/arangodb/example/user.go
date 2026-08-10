package main

import (
	"github.com/machanirobotics/loom/go/arangodb"
	"github.com/machanirobotics/loom/go/arangodb/options"
	"github.com/machanirobotics/loom/go/ulid"
)

var TestingEdge = []arangodb.Edge{
	{
		Name: "knows",
		Definition: []arangodb.EdgeDefinition{
			{
				Collection: "knows",
				From:       []string{"people"},
				To:         []string{"people"},
			},
		},
	},
	{
		Name: "likes",
		Definition: []arangodb.EdgeDefinition{
			{
				Collection: "likes",
				From:       []string{"people"},
				To:         []string{"people"},
			},
		},
	},
	{
		Name: "hates",
		Definition: []arangodb.EdgeDefinition{
			{
				Collection: "hates",
				From:       []string{"people"},
				To:         []string{"people"},
			},
		},
	},
}

var (
	dbName = "bobthebuilder"
	// --- 4. Work with Collections ---
	collectionName   = "people"
	collectionSchema = options.CollectionSchemaOptions{
		Schema: map[string]interface{}{
			"properties": map[string]interface{}{
				"name": map[string]interface{}{
					"type":        "string",
					"minLength":   1,
					"maxLength":   100,
					"description": "Name of the person",
				},
				"age": map[string]interface{}{
					"type":        "integer",
					"minimum":     0,
					"maximum":     120,
					"description": "Age of the person",
				},
			},
			"required": []string{"name"},
		},
		Level:       options.CollectionSchemaLevelStrict,
		Description: "Collection for storing people data",
	}
)

type User struct {
	ID        string `json:"id"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

func NewUser(firstname, lastname string) *User {
	return &User{
		ID:        ulid.GenerateString(),
		FirstName: firstname,
		LastName:  lastname,
	}
}
