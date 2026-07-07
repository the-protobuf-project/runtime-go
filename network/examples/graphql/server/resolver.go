package main

import (
	"sync"
	"time"
)

// Resolver is the root resolver
type Resolver struct {
	mu         sync.RWMutex
	users      map[string]*User
	posts      map[string]*Post
	nextUserID int
	nextPostID int
}

// NewResolver creates a new resolver with sample data
func NewResolver() *Resolver {
	now := time.Now().Format(time.RFC3339)

	r := &Resolver{
		users:      make(map[string]*User),
		posts:      make(map[string]*Post),
		nextUserID: 3,
		nextPostID: 3,
	}

	// Add sample users
	r.users["1"] = &User{
		ID:        "1",
		Name:      "Alice Johnson",
		Email:     "alice@example.com",
		CreatedAt: now,
		UpdatedAt: now,
	}

	r.users["2"] = &User{
		ID:        "2",
		Name:      "Bob Smith",
		Email:     "bob@example.com",
		CreatedAt: now,
		UpdatedAt: now,
	}

	// Add sample posts
	r.posts["1"] = &Post{
		ID:        "1",
		Title:     "Getting Started with GraphQL",
		Content:   "GraphQL is a query language for APIs...",
		AuthorID:  "1",
		CreatedAt: now,
		UpdatedAt: now,
	}

	r.posts["2"] = &Post{
		ID:        "2",
		Title:     "Building REST APIs",
		Content:   "REST is an architectural style...",
		AuthorID:  "2",
		CreatedAt: now,
		UpdatedAt: now,
	}

	return r
}
