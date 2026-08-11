package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/machanirobotics/loom/go/redis/shared"
	"github.com/machanirobotics/loom/go/ulid"
)

// RawPublisher handles publishing messages to streams.
type StreamPublisher struct {
	streamInfo *Stream // Stream info
	client     *Client // Redis client
}

// NotifyPublisher handles publishing messages to notification streams.
// It provides methods to publish messages with TTL (Time To Live) support.
type NotifyPublisher struct {
	notifyInfo *Stream // Stream info
	client     *Client // Redis client
}

// Publish sends a message to the specified subject in that particular stream.
func (p *StreamPublisher) Publish(subject string, message Document) error {
	shared.Pulse.Logger.Debugf("Publishing message on subject: %s", subject)
	ctx := context.Background()
	id := ulid.Generate().GetTimeCode()
	message.SetID(id)
	subjects := p.streamInfo.Subjects

	err := validateStreamSubjects(subjects, subject)
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("error in publishing:%s", err)
		return fmt.Errorf("error in publishing: %w", err)
	}
	pkey := p.streamInfo.ID() + ":" + subject

	docBytes, _, err := normalizeToJSON(message)
	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	if _, err := p.client.redisClient.Publish(ctx, pkey, docBytes).Result(); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	time.Sleep(5 * time.Second)
	if _, err := p.client.redisClient.Publish(ctx, pkey, docBytes).Result(); err != nil {
		return fmt.Errorf("failed to publish message (retry): %w", err)
	}
	shared.Pulse.Logger.Infof("Published message with key: %s", pkey)

	return nil
}

// Publish sends a message to the specified notification stream.
// The message will be automatically removed after the specified TTL.
// Returns the ID of the published message or an error if the operation fails.
func (p *NotifyPublisher) Publish(subject string, doc Document) error {
	shared.Pulse.Logger.Debugf("Publishing message on subject: %s", subject)
	ctx := context.Background()

	// ensure doc has an ID
	if doc.ID() == "" {
		id := ulid.Generate().GetTimeCode()
		doc.SetID(id)
	}

	// support comma-separated subjects
	subjects := strings.Split(subject, ",")
	val := doc.Data
	ttl := doc.TTL

	for _, s := range subjects {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if err := validateStreamSubjects(p.notifyInfo.Subjects, s); err != nil {
			return fmt.Errorf("invalid subject %s: %w", s, err)
		}

		// use per-message key so multiple messages don't overwrite each other
		ttlKey := doc.ID()
		persistKey := "doc:" + ttlKey + ":data"

		shared.Pulse.Logger.Debugf("Publishing message with key: %s", ttlKey)

		// Set the key with the value and TTL (this key will expire)
		if err := p.client.redisClient.Set(ctx, ttlKey, val, ttl).Err(); err != nil {
			return fmt.Errorf("failed to set key %s: %w", ttlKey, err)
		}

		// save one persistent entry using the same id so subscriber can fetch it
		if err2 := p.client.redisClient.Set(ctx, persistKey, val, 0).Err(); err2 != nil {
			return fmt.Errorf("failed to set key %s: %w", persistKey, err2)
		}
		shared.Pulse.Logger.Debugf("Published message with key: %s", ttlKey)

	}
	return nil
}
