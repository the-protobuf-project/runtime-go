package redis

import (
	"context"
	"fmt"
	"github.com/machanirobotics/loom/go/redis/options"
	"github.com/machanirobotics/loom/go/redis/shared"
	"github.com/machanirobotics/loom/go/ulid"
)

// StreamHandler manages the lifecycle of Redis streams.
// It provides methods to create, retrieve, update, and delete streams.
type StreamHandler struct {
	Client     *Client           // Redis client
	Publisher  *StreamPublisher  // Stream publisher
	Subscriber *StreamSubscriber // Stream subscriber
}

// Create initializes a new stream with the given configuration.
// Returns the created stream or an error if the operation fails.
func (s *StreamHandler) Create(streamreq Stream) (Stream, error) {
	shared.Pulse.Logger.Debugf("Create called with name:%s", streamreq.Name)
	ctx := context.Background()
	stream := createStreamKey(streamreq)
	jsonbytes, mappedjson, err := normalizeToJSON(stream)

	if stream.ID() == "" {
		genid := ulid.Generate().GetRandomCode()
		stream.SetID(genid)
	}
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("Error in json marshalling")
		return Stream{}, err
	}
	shared.Pulse.Logger.Debugf("Normalised json:%v", mappedjson)
	resp, err := s.Client.redisClient.XAdd(ctx, &options.XAddArgs{Stream: stream.ID(), Values: map[string]interface{}{
		kStreamInfo: jsonbytes,
	}}).Result()

	if err != nil {
		return Stream{}, err
	}
	shared.Pulse.Logger.Debugf("Stream created with ID: %s", resp)
	return stream, nil
}

// Get retrieves a stream by its ID.
// Returns the stream if found, or an error if the stream doesn't exist.
func (s *StreamHandler) Get(id string) (*Stream, error) {
	shared.Pulse.Logger.Debugf("Called get with id:%s", id)
	ctx := context.Background()
	exists, err := s.Client.redisClient.Exists(ctx, id).Result()
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("Error in checking if stream exists: %v", err)
	}
	if exists == 1 {
		res, _ := s.getStreamInfo(id)
		return res, nil
	}
	shared.Pulse.Logger.Debugf("stream with ID %s does not exist", id)
	return nil, fmt.Errorf("stream with ID %s does not exist", id)
}

// Set configures and returns a RedisChannelManager for the specified stream ID.
// This allows for publishing and subscribing to the stream.
func (s *StreamHandler) Set(id string) (*RedisStreamManager, error) {
	shared.Pulse.Logger.Debugf("Called set with id:%s", id)
	res, err := s.Get(id)
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("Error:%s", err)
	}
	if res.ID() == "" {
		res.SetID(id)
	}
	shared.Pulse.Logger.Debugf("Returning manager for stream:%v", res)
	return &RedisStreamManager{
		Publisher:  StreamPublisher{client: s.Client, streamInfo: res},
		Subscriber: StreamSubscriber{streamInfo: res, client: s.Client}}, nil
}

// Delete removes a stream by its ID.
// Returns an error if the stream doesn't exist or if the deletion fails.
func (s *StreamHandler) Delete(id string) error {
	shared.Pulse.Logger.Debugf("Called delete with id:%s", id)
	// Check if the stream exists
	keyType, err := s.Client.redisClient.Type(context.Background(), id).Result()
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("Error in checking if stream exists: %v", err)
	}
	shared.Pulse.Logger.Debugf("Key type:%s", keyType)
	if keyType != "stream" {
		_ = shared.Pulse.Logger.Errorf("stream %s does not exist, nothing to delete", id) // If the stream doesn't exist, just return
		return nil
	}

	// Delete the stream
	err = s.Client.redisClient.Del(context.Background(), id).Err()
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("failed to delete stream %s: %v", id, err)
		return fmt.Errorf("failed to delete stream: %w", err)
	}
	shared.Pulse.Logger.Debugf("deleted stream: %s", id)
	return nil
}

// Update updates a stream by its ID.
func (s *StreamHandler) Update(id string, stream Stream) (*Stream, error) {
	shared.Pulse.Logger.Debugf("Called update with id:%s", id)
	// First verify the stream exists
	_, err := s.getStreamInfo(id)
	if err != nil {
		_=shared.Pulse.Logger.Errorf("Error getting stream info for update: %v", err)
		return nil, fmt.Errorf("stream with ID %s not found", id)
	}

	if err := s.Delete(id); err != nil { // Delete the existing stream
		_=shared.Pulse.Logger.Errorf("Error deleting stream during update: %v", err)
		return nil, fmt.Errorf("failed to delete existing stream: %w", err)
	}
	stream.SetID(id)

	// Create new stream with the provided data
	if _, err := s.Create(stream); err != nil {
		_=shared.Pulse.Logger.Errorf("Error creating new stream during update: %v", err)
		return nil, fmt.Errorf("failed to create new stream: %w", err)
	}

	shared.Pulse.Logger.Debugf("Updated stream: %s", id)
	return &stream, nil
}

// List retrieves all streams in the Redis database.
// Returns a slice of Stream objects and an error if the operation fails.
func (s *StreamHandler) List() ([]Stream, error) {
	shared.Pulse.Logger.Debugf("Called list")
	var channels []Stream
	// Use SCAN to find all keys matching the stream pattern
	iter := s.Client.redisClient.Scan(context.Background(), 0, kStreamPattern, 0).Iterator()

	for iter.Next(context.Background()) {
		key := iter.Val()
		// Verify it's actually a stream
		keyType, err := s.Client.redisClient.Type(context.Background(), key).Result()
		if err != nil {
			_ = shared.Pulse.Logger.Errorf("error checking type for %s: %v", key, err)
			continue
		}
		if keyType == "stream" {
			res, err := s.getStreamInfo(key)
			if err != nil {
				_ = shared.Pulse.Logger.Errorf("error getting stream info for %s: %v", key, err)
				continue
			}
			channels = append(channels, *res)
		}
	}
	if err := iter.Err(); err != nil {
		_ = shared.Pulse.Logger.Errorf("error scanning for streams: %v", err)
		return nil, fmt.Errorf("error scanning for streams: %w", err)
	}
	shared.Pulse.Logger.Debugf("Found %d streams", len(channels))
	return channels, nil
}
