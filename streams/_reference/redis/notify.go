package redis

import (
	"context"
	"fmt"
	"github.com/machanirobotics/loom/go/redis/options"
	"github.com/machanirobotics/loom/go/redis/shared"
	"github.com/machanirobotics/loom/go/ulid"
	"log"
)

// NotifyHandler manages the creation and lifecycle of notification streams.
// It provides methods to create, retrieve, and manage notification streams
// along with their associated publishers and subscribers.
type NotifyHandler struct {
	client     *Client
	Subscriber *NotifySubscriber
	Publisher  *NotifyPublisher
}

// Create initializes a new notification stream with the given configuration.
// It creates a new stream in Redis and stores the notification metadata.
// Returns the created stream or an error if the operation fails.
func (n *NotifyHandler) Create(req Stream) (*Stream, error) {
	ctx := context.Background()
	info := createStreamKey(req)
	jsonBytes, _, err := normalizeToJSON(info)
	if err != nil {
		log.Fatalf("Error in json marshalling: %v", err)
		return nil, err
	}
	if info.ID() == "" {
		genid := ulid.Generate().GetRandomCode()
		info.SetID(genid)
	}
	// Store metadata in the key itself as a stream message "notify-info"
	resp, err := n.client.redisClient.XAdd(ctx, &options.XAddArgs{Stream: info.ID(), Values: map[string]interface{}{"notify-info": jsonBytes}}).Result()
	if err != nil {
		log.Fatalf("Error saving notify metadata: %v", err)
		return nil, err
	}
	log.Printf("Created Notify: %s (msg id: %s)", info.ID(), resp)
	return &info, nil
}

// Set creates a manager for the given key.
// It checks if the key exists (optional, but good for consistency).
// For Notify, "creating" might just mean binding to a key.
func (n *NotifyHandler) Set(id string) (*NotifyHandler, error) {
	if id == "" {
		return nil, fmt.Errorf("id cannot be empty")
	}
	// Retrieve metadata to ensure it exists and to get subjects
	info, err := n.getStream(id)
	if err != nil {
		return nil, fmt.Errorf("notify instance not found for key %s: %w", id, err)
	}
	return &NotifyHandler{
		Publisher:  &NotifyPublisher{notifyInfo: info, client: n.client},
		Subscriber: &NotifySubscriber{notifyInfo: info, client: n.client},
	}, nil
}

// Get retrieves a notification stream by its ID.
// Returns the stream information if found, or an error if the stream doesn't exist.
func (n *NotifyHandler) Get(id string) (*Stream, error) {
	// Check if the metadata exists
	info, err := n.getStream(id)
	if err != nil {
		// If key doesn't exist, getStream returns error.
		ctx := context.Background()
		exists, err := n.client.redisClient.Exists(ctx, id).Result()
		if err != nil {
			return nil, err
		}
		if exists == 0 {
			shared.Pulse.Logger.Debugf("Notify instance does NOT exist")
			return nil, fmt.Errorf("notify instance with ID %s does not exist", id)
		}

		// It exists but maybe not a stream or missing field?
		keyType, _ := n.client.redisClient.Type(ctx, id).Result()
		if keyType != "stream" {
			shared.Pulse.Logger.Debugf("Key exists but is not a stream (not a notify instance)")
			return nil, fmt.Errorf("notify instance with ID %s does not exist", id)
		}
		shared.Pulse.Logger.Debugf("Notify instance exists but failed to get info")
		return info, nil
	}
	shared.Pulse.Logger.Debugf("Notify instance exists")
	return info, nil
}

// Delete removes a notification stream and all its associated data.
// Returns an error if the stream cannot be deleted
func (n *NotifyHandler) Delete(id string) error {
	ctx := context.Background()
	// Check if exists
	exists, err := n.client.redisClient.Exists(ctx, id).Result()
	if err != nil {
		return fmt.Errorf("error checking if notify exists: %w", err)
	}
	if exists == 0 {
		log.Fatalf("notify %s does not exist, nothing to delete", id)
		return nil
	}
	// Delete the key
	err = n.client.redisClient.Del(ctx, id).Err()
	if err != nil {
		log.Fatalf("failed to delete notify %s: %v", id, err)
		return fmt.Errorf("failed to delete notify: %w", err)
	}
	log.Printf("deleted notify: %s", id)
	return nil
}

// Update applies changes to an existing notification stream's configuration.
// The specific behavior depends on the implementation.
// Update updates a stream by its ID.
func (s *NotifyHandler) Update(id string, stream Stream) (*Stream, error) {
	shared.Pulse.Logger.Debugf("Called update with id:%s", id)
	// First verify the stream exists
	_, err := s.Get(id)
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("Error getting stream info for update: %v", err)
		return nil, fmt.Errorf("stream with ID %s not found", id)
	}
	if err := s.Delete(id); err != nil { // Delete the existing stream
		_ = shared.Pulse.Logger.Errorf("Error deleting stream during update: %v", err)
		return nil, fmt.Errorf("failed to delete existing stream: %w", err)
	}
	stream.SetID(id)
	// Create new stream with the provided data
	if _, err := s.Create(stream); err != nil {
		_ = shared.Pulse.Logger.Errorf("Error creating new stream during update: %v", err)
		return nil, fmt.Errorf("failed to create new stream: %w", err)
	}
	shared.Pulse.Logger.Debugf("Updated stream: %s", id)
	return &stream, nil
}

// List returns all available notification streams.
// Returns a slice of Stream objects or an error if the operation fails.
func (n *NotifyHandler) List() ([]Stream, error) {
	ctx := context.Background()
	var notifies []Stream

	// Use SCAN to find all keys matching the pattern
	pattern := "*:*"
	iter := n.client.redisClient.Scan(ctx, 0, pattern, 0).Iterator()

	for iter.Next(ctx) {
		key := iter.Val()

		// Verify it's a stream
		keyType, err := n.client.redisClient.Type(ctx, key).Result()
		if err != nil {
			log.Printf("error checking type for %s: %v", key, err)
			continue
		}
		if keyType == "stream" {
			// Try to get notify-info
			// We can reuse getStream logic but we need to be careful not to log errors for non-notify streams
			// getStream returns empty struct if not found or not a string
			info, err := n.getStream(key)
			if err == nil && info.ID() != "" {
				notifies = append(notifies, *info)
			}
		}
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("error scanning for notifies: %w", err)
	}

	return notifies, nil
}
