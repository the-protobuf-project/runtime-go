package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/ulid"
)

var resourceNameRegex = regexp.MustCompile(`^//[^/]+/.+`)

func isResourceName(id string) bool {
	return resourceNameRegex.MatchString(strings.TrimPrefix(id, "cache:"))
}

// maskCacheID rewrites an AIP resource name //<host>/<path> to
// //cache.<host>/<path> for storage, marking the entry as the cached view of
// that resource rather than the resource itself. IDs that are not resource
// names are stored as given.
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

// Create stores a document with its TTL, generating an ID when the document
// does not carry one. A zero TTL stores an entry that does not expire.
func (c *Cache) Create(ctx context.Context, doc cache.Document) (*cache.Document, error) {
	if doc.ID() == "" {
		doc.SetID(ulid.Generate().GetRandomCode())
	}

	// The payload is stored as-is, so any JSON-serializable value works —
	// strings, numbers and slices included, not only maps and tagged structs.
	body, err := json.Marshal(doc.Data)
	if err != nil {
		return nil, fmt.Errorf("cache/redis: failed to encode document %s: %w", doc.ID(), err)
	}

	// Write the entry and its index member atomically. The entry is keyed by the
	// masked (//cache.<host>/...) form, while the index holds the caller-facing
	// form Get returns and Delete removes — indexing the raw ID instead would
	// leak the entry whenever the caller passes an already-masked resource name.
	pipe := c.rdb.TxPipeline()
	pipe.Set(ctx, c.keys.entry(maskCacheID(doc.ID())), body, doc.TTL)
	pipe.SAdd(ctx, c.keys.index(), RemoveCacheHostPrefix(doc.ID()))
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("cache/redis: failed to create %s: %w", doc.ID(), err)
	}
	return &doc, nil
}

// Get retrieves a document by ID, reporting an expired or absent entry as
// [ErrNotFound].
func (c *Cache) Get(ctx context.Context, id string) (cache.Document, error) {
	key := c.keys.entry(maskCacheID(id))

	body, err := c.rdb.Get(ctx, key).Bytes()
	if err != nil {
		return cache.Document{}, notFound(id, err)
	}

	var data any
	if err := json.Unmarshal(body, &data); err != nil {
		return cache.Document{}, fmt.Errorf("cache/redis: stored entry %s is not valid JSON: %w", id, err)
	}

	return cache.NewDocument(RemoveCacheHostPrefix(id), data, c.remainingTTL(ctx, key)), nil
}

// remainingTTL reports how much longer an entry will live, as a duration the
// caller can use directly.
//
// Redis answers TTL with two negative sentinels rather than a duration: -1 for
// a key with no expiry and -2 for a key that is gone. Both are reported as 0,
// matching the documented "a zero TTL means the entry does not expire" — a
// caller that round-tripped -1s into Update would otherwise silently clear the
// expiry, and anything treating TTL as arithmetic would see a negative
// duration.
func (c *Cache) remainingTTL(ctx context.Context, key string) time.Duration {
	ttl, err := c.rdb.TTL(ctx, key).Result()
	if err != nil || ttl < 0 {
		return 0
	}
	return ttl
}

// Update replaces the content and TTL of an existing document, reporting a
// missing entry as [ErrNotFound].
func (c *Cache) Update(ctx context.Context, id string, doc cache.Document) error {
	key := c.keys.entry(maskCacheID(id))

	body, err := json.Marshal(doc.Data)
	if err != nil {
		return fmt.Errorf("cache/redis: failed to encode document %s: %w", id, err)
	}

	// SET with XX writes only if the key is already there, so the existence
	// check and the write are one atomic step — a separate GET first would let
	// the entry expire in between and recreate it here.
	ok, err := c.rdb.SetXX(ctx, key, body, doc.TTL).Result()
	if err != nil {
		return fmt.Errorf("cache/redis: failed to update %s: %w", id, err)
	}
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return nil
}

// Delete removes a document and its index member.
//
// Deleting an entry that is not there is not an error: the caller wanted it
// gone, and a cache entry may legitimately have expired on its own a moment
// earlier.
func (c *Cache) Delete(ctx context.Context, id string) error {
	pipe := c.rdb.TxPipeline()
	pipe.Del(ctx, c.keys.entry(maskCacheID(id)))
	pipe.SRem(ctx, c.keys.index(), RemoveCacheHostPrefix(id))
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("cache/redis: failed to delete %s: %w", id, err)
	}
	return nil
}

// List returns every live document, removing index members whose entries have
// expired as it goes.
//
// Entries expire on their own but leave their index member behind, so without
// this sweep the index grows without bound in a cache with short TTLs.
func (c *Cache) List(ctx context.Context) ([]cache.Document, error) {
	ids, err := c.rdb.SMembers(ctx, c.keys.index()).Result()
	if err != nil {
		return nil, fmt.Errorf("cache/redis: failed to read index: %w", err)
	}

	docs := make([]cache.Document, 0, len(ids))
	for _, id := range ids {
		doc, err := c.Get(ctx, id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				// Expired between the index read and now: drop the stale member.
				// A failure to clean up is not worth failing the read over —
				// the next List will try again.
				_ = c.rdb.SRem(ctx, c.keys.index(), id).Err()
				continue
			}
			// A transport failure is not a stale entry; report it rather than
			// silently returning a short list.
			return nil, err
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

// Ping verifies the cache can reach its server. It is offered because New
// deliberately does not dial, so a caller that wants a readiness check has
// somewhere to make one.
func (c *Cache) Ping(ctx context.Context) error {
	if err := c.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("cache/redis: ping failed: %w", err)
	}
	return nil
}
