package main

import (
	"encoding/json"
	"log"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/machanirobotics/loom/go/google/resourcename"
	"github.com/machanirobotics/loom/go/redis"
)

// NotificationSubject represents the type of notification being sent or received.
type NotificationSubject string

const (
	// SubjectUserNotification is used for general user-facing notifications
	SubjectUserNotification NotificationSubject = "UserNotification"
	SubjectSystemAlert      NotificationSubject = "SystemAlert"
)

func (s NotificationSubject) String() string {
	return string(s)
}

// notificationMessage represents the notification message structure
type notificationMessage struct {
	_       struct{} `resource:"//machanirobotics.com/notification/{subject}/{user_id}"`
	Subject string   `json:"subject" resource:"subject"`
	UserID  string   `json:"user_id" resource:"user_id"`
	Message string   `json:"message"`
}

func main() {
	// Database configuration
	dbName := "notifyexampledb"

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

	// Create a new notification stream with specific subjects
	n, err := manager.Channel.Notify.Create(redis.Stream{
		Name:        "alerts",
		Description: "notify instance for system alerts",
		Subjects:    []string{string(SubjectUserNotification), string(SubjectSystemAlert)},
		UserID:      "testuser",
	})
	if err != nil {
		log.Fatalf("Failed to create notify instance: %v", err)
	}

	// Retrieve the newly created notify instance to verify it was created
	notify, err := manager.Channel.Notify.Get(n.ID())
	if err != nil {
		log.Fatalf("Failed to get notify instance: %v", err)
	}
	log.Printf("Retrieved notify instance: %+v\n", notify)

	// List all available notify instances
	notifications, err := manager.Channel.Notify.List()
	if err != nil {
		log.Printf("Failed to list notify instances: %v", err)
	} else {
		log.Printf("List of notify instances: %+v\n", notifications)
	}

	// Get a manager for the notify instance to perform pub/sub operations
	notifyMgr, err := manager.Channel.Notify.Set(n.ID())
	if err != nil {
		log.Fatalf("Failed to set notify instance: %v", err)
	}

	// --- SUBSCRIBER SETUP ---
	// Subscribe to notifications for the UserNotification subject
	msgChan, err := notifyMgr.Subscriber.Subscribe(SubjectUserNotification.String())
	if err != nil {
		log.Fatalf("Subscribe error: %v", err)
	}

	// Start a goroutine to listen for incoming notifications
	go func() {
		// This loop will run until the message channel is closed
		for msg := range msgChan {
			log.Printf(">> RECEIVED NOTIFICATION: %+v\n", msg)
		}
	}()

	// --- PUBLISHER ---
	log.Println("Publishing notification with 5 second TTL...")

	// Create a message to be published
	messageData := notificationMessage{
		Subject: string(SubjectUserNotification),
		UserID:  "testuser",
		Message: "System maintenance scheduled for tonight at 2 AM",
	}

	// Marshal the struct to JSON bytes
	messageBytes, err := json.Marshal(messageData)
	if err != nil {
		log.Fatalf("Error marshaling message: %v", err)
	}

	// Use the resource name as the document ID, similar to cache example
	docID, err := resourcename.MarshalResource(messageData)
	if err != nil {
		log.Fatalf("Error building resource name: %s", err)
	}

	// Create document with ULID-based ID and JSON bytes data, similar to cache example
	doc := redis.Document{
		Data: messageBytes,
		TTL:  5 * time.Second,
	}
	doc.SetID(docID)

	// Publish the message with a 5-second TTL using ULID-based ID
	err = notifyMgr.Publisher.Publish(string(SubjectUserNotification), doc)
	if err != nil {
		log.Fatalf("Publish error: %v", err)
	}
	log.Printf("Published message with ULID ID: %s", docID)

	time.Sleep((2 * time.Second))

	// Publish second message
	doc2 := redis.Document{
		Data: messageBytes,
		TTL:  5 * time.Second,
	}
	doc2.SetID(docID)

	err = notifyMgr.Publisher.Publish(string(SubjectUserNotification), doc2)
	if err != nil {
		log.Fatalf("Publish error: %v", err)
	}

	log.Printf("Published message: %s (will expire in 5 seconds)", messageData.Message)
	log.Println("Notification published. Waiting for it to expire...")

	// Keep the program running long enough to see the notification
	// The notification will be received after the TTL expires (5 seconds)
	select {}
}
