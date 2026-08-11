package main

import (
	"log"

	"github.com/machanirobotics/loom/go/redis"
)

// NudgeSubject represents the type of notification being sent through the stream.
type NudgeSubject string

const (
	SubjectPillReminder NudgeSubject = "PillReminder"

	SubjectHealthTip NudgeSubject = "HealthTip"
)

// String returns the string representation of the NudgeSubject.
// This implements the Stringer interface for better logging and display.
func (s NudgeSubject) String() string {
	return string(s)
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

	// STREAM CREATION
	log.Println("Creating a new stream...")
	s, err := manager.Channel.Stream.Create(redis.Stream{
		Name:        "reminders",
		Description: "stream for reminders",
		Subjects:    []string{string(SubjectPillReminder)},
		UserID:      "testuser",
	})
	if err != nil {
		log.Fatalf("Failed to create stream: %v", err)
	}
	log.Printf("Created stream with ID: %s\n", s.ID())

	// STREAM RETRIEVAL
	stream, err := manager.Channel.Stream.Get(s.ID())
	if err != nil {
		log.Fatalf("Failed to get stream: %v", err)
	}
	log.Printf("Retrieved stream: %+v\n", stream)

	// UPDATE STREAM
	updatedStream, err := manager.Channel.Stream.Update(s.ID(), redis.Stream{
		Name:        "reminders",
		Description: "updated stream for reminders",
		Subjects:    []string{string(SubjectPillReminder)},
		UserID:      "testuser",
	})
	if err != nil {
		log.Fatalf("Failed to update stream: %v", err)
	}
	log.Printf("Updated stream: %+v\n", updatedStream)

	// LIST STREAMS
	streams, err := manager.Channel.Stream.List()
	if err != nil {
		log.Printf("Failed to list streams: %v", err)
	} else {
		log.Printf("Available streams: %+v\n", streams)
	}
	
	//GET STREAM MANAGER
	//The stream manager provides methods for publishing and subscribing
	mgr, err := manager.Channel.Stream.Set(s.ID())
	if err != nil {
		log.Fatalf("Failed to get stream manager: %v", err)
	}
	log.Printf("Stream manager initialized for stream ID: %s\n", s.ID())

	// SUBSCRIBER SETUP
	// Subscribe to messages for the PillReminder subject
	msgChan, err := mgr.Subscriber.Subscribe(string(SubjectPillReminder))
	if err != nil {
		log.Fatalf("Failed to subscribe to stream: %v", err)
	}

	// Start a goroutine to handle incoming messages
	go func() {
		log.Printf("Subscriber started, waiting for messages...")
		for msg := range msgChan {
			log.Printf(">> RECEIVED MESSAGE: %+v\n", msg)
		}
	}()

	// PUBLISHER
	// Publish a message to the stream
	//messgae should be a struct
	content := map[string]interface{}{
		"data": "Take a pill, dont miss da",
	}
	err = mgr.Publisher.Publish(string(SubjectPillReminder), redis.Document{
		Data: content,
	})
	if err != nil {
		log.Fatalf("Failed to publish message: %v", err)
	}

	log.Printf("Message published: %s", content)
	log.Println("Stream is active. Press Ctrl+C to exit.")

	// Keep the program running to receive messages
	select {}
}
