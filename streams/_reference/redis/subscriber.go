package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/machanirobotics/loom/go/redis/shared"
)

// StreamSubscriber handles subscription to Redis streams and pub/sub channels.
// It provides methods to subscribe to messages and receive them asynchronously.
type StreamSubscriber struct {
	streamInfo *Stream // stream info
	client     *Client // redis client
}

// NotifySubscriber handles subscribing to notification streams.
// It provides methods to subscribe to notifications and receive messages.
type NotifySubscriber struct {
	client     *Client // Redis client
	notifyInfo *Stream // Stream info
}

// Subscribe subscribes to a stream.
func (s *StreamSubscriber) Subscribe(subject string) (response chan Document, err error) {

	err = validateStreamSubjects(s.streamInfo.Subjects, subject)
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("error in subscribing:%s", err)
		return nil, fmt.Errorf("error in subscribing: %w", err)
	}
	pattern := s.streamInfo.ID() + ":" + subject
	shared.Pulse.Logger.Debugf("Subject Exists subscribing")
	return subscribe(pattern, s.client)
}

// Subscribe subscribes to a notification stream.
func (s *NotifySubscriber) Subscribe(subject string) (response chan Document, err error) {
	shared.Pulse.Logger.Debugf("Subject Exists subscribing")
	return subscribeKeyEvents(s.client)
}

// Subscribe subscribes to a stream.
func subscribe(subject string, client *Client) (msgChan chan Document, err error) {
	ctx := context.Background()
	rdb := client.redisClient

	sub := rdb.Subscribe(ctx, subject)

	// Confirm the subscription is active
	_, err = sub.Receive(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}

	// Create a channel to send parsed messages
	msgChan = make(chan Document)

	go func() {
		defer close(msgChan)
		defer func() {
			_ = sub.Close()
		}()

		for m := range sub.Channel() {
			payload := m.Payload
			// Try JSON first
			var doc Document
			if err := json.Unmarshal([]byte(payload), &doc); err == nil {
				doc.id = subject
				msgChan <- doc
				shared.Pulse.Logger.Infof("Received document: %v", doc)
				continue
			}

			// Fallback: treat payload as a key
			if data, ok := fetchFromKey(ctx, client, payload+":data"); ok {
				doc.id = m.Payload
				doc.Data = data
				msgChan <- doc
				shared.Pulse.Logger.Infof("Received document: %v", doc)
				continue
			}
			_ = shared.Pulse.Logger.Errorf("Invalid message: %s", payload)
		}
	}()
	return msgChan, nil
}

// subscribeKeyEvents subscribes to Redis key-event notifications for expired keys
// and forwards persistent data saved under <expired-key>:data.
// Now handles simplified key structure where keys are just doc.ID().
func subscribeKeyEvents(client *Client) (msgChan chan Document, err error) {
	ctx := context.Background()
	rdb := client.redisClient

	pattern := fmt.Sprintf(kSubscribePattern, client.redisClient.Options().DB)
	sub := rdb.Subscribe(ctx, pattern)

	_, err = sub.Receive(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to keyevents: %w", err)
	}

	msgChan = make(chan Document)

	go func() {
		defer close(msgChan)
		defer func() { _ = sub.Close() }()

		for m := range sub.Channel() {
			payload := strings.TrimSpace(m.Payload)
			// With simplified key structure, payload is just the docID
			docID := payload

			// fetch persistent data saved under <expired-key>:data
			persistKey := "doc:" + payload + ":data"
			if data, found := fetchFromKey(ctx, client, persistKey); found {
				var doc Document
				doc.SetID(docID)
				doc.Data = data
				msgChan <- doc
				shared.Pulse.Logger.Infof("Received persisted document for key %s: %v", persistKey, doc)
				continue
			}
			shared.Pulse.Logger.Debugf("No persistent data found for expired key: %s", payload)
		}
	}()
	return msgChan, nil
}

// fetchFromKey retrieves the value of a key from Redis.
func fetchFromKey(ctx context.Context, rdb *Client, key string) (string, bool) {
	data, err := rdb.redisClient.Get(ctx, key).Result()
	if err != nil {
		return "empty", false
	}
	return data, true
}
