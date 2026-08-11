package redis

import (
	"context"
	"fmt"

	"github.com/machanirobotics/loom/go/redis/shared"
)

// kvHandler implements DocumentManager and provides a Redis-backed key-value document store.
type kvHandler struct {
	client *Client // Redis client
}

// Create creates a new document in Redis with deduplication.
func (c *kvHandler) Create(document Document) (*Document, error) {
	rdb := c.client.redisClient

	hashResult, err := c.verifyHash(document)
	if err != nil {
		return nil, err
	}

	// Write new document and indices
	if err := c.client.writeNewDoc(context.Background(), hashResult.genKey, hashResult.hash, hashResult.normalisedJson); err != nil {
		// Rollback hash reservation on failure
		if delErr := rdb.Del(context.Background(), kHash(hashResult.hash)).Err(); delErr != nil {
			shared.Pulse.Logger.Warnf("failed to rollback hash reservation %s: %v", hashResult.hash, delErr)
		}
		return nil, err
	}
	document.SetID(hashResult.genKey)
	return &document, nil
}

// Get retrieves a document by its ID.Returns an error if the document does not exist.
func (c *kvHandler) Get(id string) (Document, error) {
	doc, err := c.client.readDocAsMap(context.Background(), id)
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("failed to get document %s: %v", id, err)
		return Document{}, err
	}
	if doc == nil {
		return Document{}, fmt.Errorf("document %s not found", id)
	}
	return Document{id: id, Data: doc}, nil
}

// Update updates an existing document with new content.
// Rejects the update if another document with the same content exists.
func (c *kvHandler) Update(id string, content Document) error {
	rdb := c.client.redisClient
	ctx := context.Background()
	docKey := kDoc(id)

	existing, err := rdb.Get(ctx, docKey).Result()
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("failed to check existing document: %v", err)
		return fmt.Errorf("failed to check existing document: %w", err)
	}
	if existing == "" {
		return fmt.Errorf("document %s not found", id)
	}

	// Normalize and hash new content
	_, asMap, err := normalizeToJSON(content)
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("failed to normalize content: %v", err)
		return fmt.Errorf("failed to normalize content: %w", err)
	}
	newHash, canonJSON, err := hashContent(asMap)
	if err != nil {
		return fmt.Errorf("failed to hash content: %w", err)
	}
	// Reject if hash belongs to another document
	if existingID, err := rdb.Get(ctx, kHash(newHash)).Result(); err == nil && existingID != "" && existingID != id {
		return fmt.Errorf("duplicate content exists in document %s", existingID)
	}

	// Update document and indices atomically
	oldHash, _ := rdb.Get(ctx, kDocHash(id)).Result()
	pipe := rdb.TxPipeline()
	pipe.Set(ctx, docKey, canonJSON, 0)
	pipe.Set(ctx, kDocHash(id), newHash, 0)
	pipe.Set(ctx, kHash(newHash), id, 0)
	if oldHash != "" && oldHash != newHash {
		pipe.Del(ctx, kHash(oldHash))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		_ = shared.Pulse.Logger.Errorf("update failed for id=%s: %v", id, err)
		return fmt.Errorf("update failed: %w", err)
	}
	return nil
}

// Delete deletes a document and all associated indices.
// Returns an error if the delete operation fails.
func (c *kvHandler) Delete(id string) error {
	rdb := c.client.redisClient
	ctx := context.Background()
	docKey := kDoc(id)

	existing, err := rdb.Exists(ctx, docKey).Result()
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("failed to check existing document: %v", err)
		return fmt.Errorf("failed to check document existence: %w", err)
	}
	if existing == 0 {
		shared.Pulse.Logger.Debugf("document not found: %s", id)
		return fmt.Errorf("document %s not found", id)
	}
	// Delete document and all indices atomically
	oldHash, _ := rdb.Get(ctx, kDocHash(id)).Result()
	pipe := rdb.TxPipeline()
	pipe.Del(ctx, docKey)
	pipe.SRem(ctx, kIndex, id)
	pipe.Del(ctx, kDocHash(id))
	if oldHash != "" {
		pipe.Del(ctx, kHash(oldHash))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		_ = shared.Pulse.Logger.Errorf("delete failed for id=%s: %v", id, err)
		return fmt.Errorf("delete failed: %w", err)
	}
	return nil
}

// List returns all stored documents.
// Automatically cleans up stale IDs found in the index.
func (c *kvHandler) List() ([]Document, error) {
	rdb := c.client.redisClient
	ctx := context.Background()
	ids, err := rdb.SMembers(ctx, kIndex).Result()
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("failed to list documents: %v", err)
		return nil, fmt.Errorf("failed to list documents: %w", err)
	}
	out := make([]Document, 0, len(ids))
	for _, id := range ids {
		m, err := c.client.readDocAsMap(ctx, id)
		if err != nil {
			if err.Error() == fmt.Sprintf("document %s not found", id) {
				if err := rdb.SRem(ctx, kIndex, id).Err(); err != nil {
					shared.Pulse.Logger.Warnf("ListDocuments: failed to remove stale ID %s: %v", id, err)
				}
				continue
			}
			shared.Pulse.Logger.Warnf("ListDocuments: skip id=%s error=%v", id, err)
			continue
		}
		out = append(out, Document{id: id, Data: m})
	}
	return out, nil
}
