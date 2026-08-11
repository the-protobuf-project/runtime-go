package redis

import "fmt"

// Database Related Utilities.
func kDB(name string) string { return "db:" + name }
func kDBID(id int64) string  { return fmt.Sprintf("dbid:%d", id) }

const kDBCounter = "db:counter"

// kIndex is the key for the set containing all document IDs.
// Data Type: Set.
const kIndex = "docs:index"

// kCacheIndex is the Redis set key used to track all cache item IDs.
const kCacheIndex = "caches:index"

// kDoc returns the key for storing a document's body.
// Data Type: String (JSON).
func kDoc(id string) string { return "doc:" + id }

// kHash returns the key for the content-hash-to-ID index. This enables
// content-based addressing and deduplication.
// Data Type: String.
func kHash(h string) string { return "docs:content:" + h }

// kDocHash returns the key for the ID-to-content-hash index.
// Data Type: String.
func kDocHash(id string) string { return "dochash:" + id }

// Cache Related Utilities.
// kCache returns the Redis key used to store the cache item
// associated with the given ID.
func kCache(id string) string { return "cache:" + id }

// Stream Related Utilities.
const kStreamInfo = "stream-info" // Field name for storing stream metadata in Redis streams.
const kStreamPattern = "*:*"      // Pattern to match all stream keys in Redis.

// Notification Related Utilities.
const kSubscribePattern = "__keyevent@%d__:expired" // Pattern for keyspace notifications on expired keys.
