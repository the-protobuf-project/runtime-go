package main

import (
	"fmt"
	"log"
	"time"

	"github.com/the-protobuf-project/runtime-go/network"
)

// This version uses GraphQL variables (proper GraphQL approach)

func main() {
	fmt.Println("=== GraphQL Local Server Examples (Simple) ===")
	fmt.Println("Testing with local GraphQL server at http://localhost:8080/query")

	// Create GraphQL client
	netConn, err := network.NewConnection(network.GraphQLConnClient)
	if err != nil {
		log.Fatalf("Failed to create network connection: %v", err)
	}
	defer func() { _ = netConn.Close() }()

	// Configure connection to gqlgen GraphQL server
	opts := network.ConnectionOptions{
		URL: network.URLOptions{
			Scheme: network.HTTP,
			Host:   "localhost:8080",
			Paths:  []string{"/query"},
		},
		Timeout: 15 * time.Second,
	}

	_, err = netConn.WithOpts(opts)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	graphqlClient, err := netConn.AsGraphQLConnectionType()
	if err != nil {
		log.Fatalf("Failed to cast to GraphQL client: %v", err)
	}

	fmt.Println("Connected to GraphQL server successfully!")
	fmt.Println()

	// Run examples
	simpleQuery1_GetAllUsers(graphqlClient)
	simpleQuery2_GetUserByID(graphqlClient)
	simpleMutation1_CreateUser(graphqlClient)
	simpleMutation2_CreatePost(graphqlClient)
	simpleMutation3_UpdateUser(graphqlClient)
	simpleQuery3_VerifyAll(graphqlClient)
}

// Simple query - no variables
func simpleQuery1_GetAllUsers(client *network.GraphQLClient) {
	fmt.Println("1. Query - Get All Users (no variables)")

	var query struct {
		Users []struct {
			ID        string
			Name      string
			Email     string
			CreatedAt time.Time
		} `graphql:"users"`
	}

	resultChan := client.Query(&query, nil)
	result := <-resultChan

	if result.Error != nil {
		log.Printf("   ERROR: %v\n\n", result.Error)
		return
	}

	fmt.Printf("   SUCCESS: Found %d users\n", len(query.Users))
	for _, user := range query.Users {
		fmt.Printf("   - %s (%s)\n", user.Name, user.Email)
	}
	fmt.Println()
}

// Query with variables
func simpleQuery2_GetUserByID(client *network.GraphQLClient) {
	fmt.Println("2. Query - Get User by ID (with variables)")

	var query struct {
		User struct {
			ID    string
			Name  string
			Email string
			Posts []struct {
				ID    string
				Title string
			}
		} `graphql:"user(id: $id)"`
	}

	// Use network.ID type for ID variables
	variables := map[string]interface{}{
		"id": network.ID("1"),
	}

	resultChan := client.Query(&query, variables)
	result := <-resultChan

	if result.Error != nil {
		log.Printf("   ERROR: %v\n\n", result.Error)
		return
	}

	fmt.Printf("   SUCCESS: User: %s\n", query.User.Name)
	fmt.Printf("   Posts: %d\n", len(query.User.Posts))
	fmt.Println()
}

// Input type for CreateUser
type CreateUserInput struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// Mutation with variables
func simpleMutation1_CreateUser(client *network.GraphQLClient) {
	fmt.Println("3. Mutation - Create User (with variables)")

	var mutation struct {
		CreateUser struct {
			ID        string
			Name      string
			Email     string
			CreatedAt string
		} `graphql:"createUser(input: $input)"`
	}

	input := CreateUserInput{
		Name:  "Charlie Brown",
		Email: "charlie@example.com",
	}

	variables := map[string]interface{}{
		"input": input,
	}

	resultChan := client.Mutation(&mutation, variables)
	result := <-resultChan

	if result.Error != nil {
		log.Printf("   ERROR: %v\n\n", result.Error)
		return
	}

	fmt.Printf("   SUCCESS: Created user %s\n", mutation.CreateUser.Name)
	fmt.Printf("   ID: %s\n", mutation.CreateUser.ID)
	fmt.Println()
}

// Input type for CreatePost
type CreatePostInput struct {
	Title    string `json:"title"`
	Content  string `json:"content"`
	AuthorID string `json:"authorId"`
}

// Mutation - create post with variables
func simpleMutation2_CreatePost(client *network.GraphQLClient) {
	fmt.Println("4. Mutation - Create Post (with variables)")

	var mutation struct {
		CreatePost struct {
			ID    string
			Title string
		} `graphql:"createPost(input: $input)"`
	}

	input := CreatePostInput{
		Title:    "Test Post",
		Content:  "Test content",
		AuthorID: "1",
	}

	variables := map[string]interface{}{
		"input": input,
	}

	resultChan := client.Mutation(&mutation, variables)
	result := <-resultChan

	if result.Error != nil {
		log.Printf("   ERROR: %v\n\n", result.Error)
		return
	}

	fmt.Printf("   SUCCESS: Created post: %s\n", mutation.CreatePost.Title)
	fmt.Println()
}

// Input type for UpdateUser
type UpdateUserInput struct {
	ID   string  `json:"id"`
	Name *string `json:"name,omitempty"`
}

// Mutation - update user with variables
func simpleMutation3_UpdateUser(client *network.GraphQLClient) {
	fmt.Println("5. Mutation - Update User (with variables)")

	var mutation struct {
		UpdateUser struct {
			ID        string
			Name      string
			UpdatedAt string
		} `graphql:"updateUser(input: $input)"`
	}

	name := "Alice Johnson Updated"
	input := UpdateUserInput{
		ID:   "1",
		Name: &name,
	}

	variables := map[string]interface{}{
		"input": input,
	}

	resultChan := client.Mutation(&mutation, variables)
	result := <-resultChan

	if result.Error != nil {
		log.Printf("   ERROR: %v\n\n", result.Error)
		return
	}

	fmt.Printf("   SUCCESS: Updated user: %s\n", mutation.UpdateUser.Name)
	fmt.Println()
}

// Verify all
func simpleQuery3_VerifyAll(client *network.GraphQLClient) {
	fmt.Println("6. Query - Verify All Data")

	var query struct {
		Users []struct {
			ID   string
			Name string
		} `graphql:"users"`
		Posts []struct {
			ID    string
			Title string
		} `graphql:"posts"`
	}

	resultChan := client.Query(&query, nil)
	result := <-resultChan

	if result.Error != nil {
		log.Printf("   ERROR: %v\n\n", result.Error)
		return
	}

	fmt.Printf("   SUCCESS: Total users: %d, Total posts: %d\n", len(query.Users), len(query.Posts))
	fmt.Println()
}
