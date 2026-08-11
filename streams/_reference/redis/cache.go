package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/machanirobotics/loom/go/redis/shared"
	"github.com/machanirobotics/loom/go/ulid"
)

// cacheHandler implements DocumentManager for a Redis-backed cache store.
type cacheHandler struct {
	client *Client
}

var resourceNameRegex = regexp.MustCompile(`^//[^/]+/.+`)

func isResourceName(id string) bool {
	return resourceNameRegex.MatchString(strings.TrimPrefix(id, "cache:"))
}

func maskCacheID(id string) string {
	trimmed := strings.TrimPrefix(id, "cache:")
	if !isResourceName(trimmed) {
		return trimmed
	}
	withoutPrefix := strings.TrimPrefix(trimmed, "//")
	parts := strings.SplitN(withoutPrefix, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return trimmed
	}
	host := parts[0]
	if !strings.HasPrefix(host, "cache.") {
		host = "cache." + host
	}
	return "//" + host + "/" + parts[1]
}

// RemoveCacheHostPrefix converts //cache.<domain>/<path> to //<domain>/<path>.
// If the host does not start with "cache.", the input is returned unchanged.
func RemoveCacheHostPrefix(resourceName string) string {
	trimmed := strings.TrimPrefix(resourceName, "//")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) != 2 {
		return resourceName
	}

	host := strings.TrimPrefix(parts[0], "cache.")
	return "//" + host + "/" + parts[1]
}

// Create creates a new cache entry with TTL support.
// Content is normalized to JSON and assigned a ULID-based ID unless an ID is already provided.
func (c *cacheHandler) Create(document Document) (*Document, error) {
	ctx := context.Background()
	rdb := c.client.redisClient
	if rdb == nil {
		_ = shared.Pulse.Logger.Errorf("Error: %v", fmt.Errorf("redis client not found"))
		return nil, fmt.Errorf("redis client not found")
	}
	if document.ID() == "" {
		genKey := ulid.Generate().GetRandomCode()
		document.SetID(genKey)
		shared.Pulse.Logger.Debugf("Generated new cache key: %s, TTL: %v", document.ID(), document.TTL)
	} else {
		shared.Pulse.Logger.Debugf("Using provided cache key: %s, TTL: %v", document.ID(), document.TTL)
	}

	jsonBytes, _, err := normalizeToJSON(document.Data)
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("Error normalizing content: %v", err)
		return nil, fmt.Errorf("failed to normalize content: %w", err)
	}
	// Store cache entry and add to index atomically
	storageID := maskCacheID(document.ID())
	pipe := rdb.TxPipeline()
	pipe.Set(ctx, kCache(storageID), jsonBytes, document.TTL)
	pipe.SAdd(ctx, kCacheIndex, document.ID())
	results, err := pipe.Exec(ctx)
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("Error executing pipeline for key %s: %v", storageID, err)
		return nil, fmt.Errorf("failed to create cache item: %w", err)
	}
	shared.Pulse.Logger.Debugf("Successfully created cache item: %s, operations completed: %d", document.ID(), len(results))
	return &document, nil
}

// Get retrieves a cache entry by ID.
// Returns an error if the entry does not exist.
func (c *cacheHandler) Get(id string) (Document, error) {
	shared.Pulse.Logger.Debugf("Retrieving cache item: %s", id)
	rdb := c.client.redisClient
	if rdb == nil {
		_ = shared.Pulse.Logger.Errorf("Error: %v", fmt.Errorf("redis client not found"))
		return Document{}, fmt.Errorf("redis client not found")
	}
	ctx := context.Background()
	storageID := maskCacheID(id)
	val, err := rdb.Get(ctx, kCache(storageID)).Bytes()
	if err != nil {
		shared.Pulse.Logger.Debugf("Cache miss for key: %s, error: %v", id, err)
		return Document{}, c.client.notFoundIfNil(kCache(storageID), id, err)
	}
	var m interface{}
	if err := json.Unmarshal(val, &m); err != nil {
		err = fmt.Errorf("cached item is not valid JSON: %w", err)
		_ = shared.Pulse.Logger.Errorf("Error unmarshaling cache item %s: %v", id, err)
		return Document{}, err
	}
	remaining, err := rdb.TTL(ctx, kCache(storageID)).Result()
	if err != nil {
		remaining = 0
	}
	return Document{id: RemoveCacheHostPrefix(id), Data: m, TTL: remaining}, nil
}

// Update updates an existing cache entry with new content and TTL.
// Returns an error if the entry does not exist.
func (c *cacheHandler) Update(id string, content Document) error {
	shared.Pulse.Logger.Debugf("Starting update for cache item: %s", id)

	rdb := c.client.redisClient
	if rdb == nil {
		_ = shared.Pulse.Logger.Errorf("Error: %v", fmt.Errorf("redis client not found"))
		return fmt.Errorf("redis client not found")
	}
	ctx := context.Background()
	storageID := maskCacheID(id)
	docKey := kCache(storageID)
	shared.Pulse.Logger.Debugf("Checking existence of cache item: %s", id)
	existing, err := rdb.Get(ctx, docKey).Result()
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("Error checking document %s: %v", id, err)
		return fmt.Errorf("failed to check existing document: %w", err)
	}
	if existing == "" {
		shared.Pulse.Logger.Warnf("document %s not found", id)
		return fmt.Errorf("document %s not found", id)
	}
	jsonBytes, _, err := normalizeToJSON(content.Data)
	if err != nil {
		err = fmt.Errorf("failed to normalize content: %w", err)
		_ = shared.Pulse.Logger.Errorf("Error normalizing content for %s: %v", id, err)
		return err
	}
	shared.Pulse.Logger.Debugf("Updating cache item: %s, size: %d bytes, TTL: %v",
		id, len(jsonBytes), content.TTL)

	if err := rdb.Set(ctx, kCache(storageID), jsonBytes, content.TTL).Err(); err != nil {
		err = fmt.Errorf("failed to update cache item: %w", err)
		_ = shared.Pulse.Logger.Errorf("Error updating cache item %s: %v", storageID, err)
		return err
	}
	return nil
}

// Delete removes a cache entry and its index reference atomically.
// Returns an error if the entry does not exist.
func (c *cacheHandler) Delete(id string) error {
	shared.Pulse.Logger.Debugf("Starting deletion of cache item: %s", id)
	ctx := context.Background()
	rdb := c.client.redisClient
	if rdb == nil {
		_ = shared.Pulse.Logger.Errorf("Error: %v", fmt.Errorf("redis client not found"))
		return fmt.Errorf("redis client not found")
	}
	shared.Pulse.Logger.Debugf("Executing delete pipeline for key: %s", id)
	storageID := maskCacheID(id)
	pipe := rdb.TxPipeline()
	pipe.Del(ctx, kCache(storageID))
	pipe.SRem(ctx, kCacheIndex, RemoveCacheHostPrefix(id))
	results, err := pipe.Exec(ctx)

	if err != nil {
		err = fmt.Errorf("failed to delete cache item %s: %w", id, err)
		_ = shared.Pulse.Logger.Errorf("Error executing delete pipeline: %v", err)
		return err
	}
	shared.Pulse.Logger.Debugf("Successfully deleted cache item: %s, operations completed: %d",
		id, len(results))
	return nil
}

// List returns all cache entries.Automatically cleans up stale IDs from the index.
func (c *cacheHandler) List() ([]Document, error) {
	rdb := c.client.redisClient
	if rdb == nil {
		_ = shared.Pulse.Logger.Errorf("Error: %v", fmt.Errorf("redis client not found"))
		return nil, fmt.Errorf("redis client not found")
	}
	startTime := time.Now()
	ctx := context.Background()
	ids, err := rdb.SMembers(ctx, kCacheIndex).Result()
	if err != nil {
		err = fmt.Errorf("failed to list cache items: %w", err)
		_ = shared.Pulse.Logger.Errorf("Error retrieving cache index: %v", err)
		return nil, err
	}
	shared.Pulse.Logger.Debugf("Found %d items in cache index", len(ids))
	caches := make([]Document, 0, len(ids))
	staleCount := 0
	for _, id := range ids {
		cache, err := c.Get(id)
		if err != nil {
			if err.Error() == fmt.Sprintf("document %s not found", id) {
				staleCount++
				shared.Pulse.Logger.Debugf("Found stale index entry: %s, removing from index", id)
				if err := rdb.SRem(ctx, kCacheIndex, id).Err(); err != nil {
					shared.Pulse.Logger.Warnf("Failed to remove stale ID %s from index: %v", id, err)
				}
			}
			continue
		}
		caches = append(caches, cache)
	}
	shared.Pulse.Logger.Debugf("List operation completed in %v. Found %d valid items, %d stale entries",
		time.Since(startTime), len(caches), staleCount)
	return caches, nil
}
