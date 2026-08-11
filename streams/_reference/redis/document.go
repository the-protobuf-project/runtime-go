package redis

import (
	"sync"
)

var (
	// defaultOnce ensures the documentHandler is initialized only once.
	defaultOnce sync.Once
	// defaultDocumentHandler is the singleton instance of the manager.
	defaultDocumentHandler *documentHandler
)

// DocumentManager defines the interface for a Redis-backed
// key-value document store. It supports creation, retrieval,
// updating, deletion, and listing of documents.
type DocumentManager interface {
	// CreateDocument creates a new document. The document content
	// is normalized and deduplicated using a content hash.
	// Returns the stored document ID and content, or an error.
	Create(document Document) (*Document, error)

	// GetDocument retrieves a document by its ID. Returns an error
	// if the document does not exist or cannot be decoded.
	Get(id string) (Document, error)

	// UpdateDocument updates an existing document with new content.
	// The content is normalized and hashed for deduplication. If
	// another document with the same content exists, the update
	// fails.
	Update(id string, content Document) error

	// DeleteDocument removes a document and all associated hash
	// and index entries. Returns an error if deletion fails.
	Delete(id string) error

	// ListDocuments returns all stored documents. Stale or missing
	// entries in the index are automatically cleaned up.
	List() ([]Document, error)
}

// documentHandler manages document operations for different document types
// (KV and Cache) and provides thread-safe access to their corresponding
// managers.
type documentHandler struct {
	// redisClient is the underlying Redis client instance used by all
	// document managers to perform read/write operations.
	redisClient *Client

	// Cache provides direct access to the document manager responsible
	// for handling cached documents (documents with TTL).
	Cache DocumentManager

	// KV provides direct access to the document manager responsible for
	// basic key-value document operations (no TTL).
	KV DocumentManager
}

// GetDocumentHandler returns the singleton instance of documentHandler, initializing it if necessary.
// It's safe for concurrent use by multiple goroutines.
func GetDocumentHandler(client *Client) *documentHandler {
	if defaultDocumentHandler == nil {
		defaultOnce.Do(func() {
			defaultDocumentHandler = &documentHandler{
				redisClient: client,
				KV:          newKVHandler(client),
				Cache:       newCacheHandler(client),
			}
		})
	}
	return defaultDocumentHandler
}

// forcing the kvHandler to implement DocumentManager interface.
func newKVHandler(client *Client) DocumentManager {
	return &kvHandler{client: client}
}

// forcing the cacheHandler to implement DocumentManager interface.
func newCacheHandler(client *Client) DocumentManager {
	return &cacheHandler{client: client}
}
