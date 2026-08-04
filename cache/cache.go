// Package cache defines the backend-agnostic contract for runtime-go's cache
// layer: ephemeral, TTL-bound storage.
//
// Providers live in subpackages — [github.com/the-protobuf-project/runtime-go/cache/redis]
// and friends — and each exposes a typed Config plus a New constructor:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 2})
//	c, err := cacheredis.New(cacheredis.Config{Client: rdb, Prefix: "orders"})
//
// A provider never opens its own connection. The caller builds the client,
// which is what makes the client's lifetime, pooling, database index, and
// shutdown the caller's to control — and what keeps this package free of any
// process-wide connection state. Two caches built from two different clients
// are genuinely independent.
//
// Cross-cutting behavior composes around any provider rather than being
// reimplemented inside each one:
//
//	c = cache.WithRetry(c, 3, 100*time.Millisecond)
//	c = cache.WithTelemetry(c, meter)
//
// For durable, non-expiring records see the sibling
// [github.com/the-protobuf-project/runtime-go/database] module; for messaging,
// [github.com/the-protobuf-project/runtime-go/streams].
package cache

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when an entry does not exist — including when it has
// expired, which for a cache is the same observable outcome. Providers wrap it
// with the key for context, so callers test with errors.Is rather than
// comparing formatted error strings.
var ErrNotFound = errors.New("cache: not found")

// Document is an entry stored in a cache: a JSON-serializable payload plus a
// TTL. A zero TTL means the entry does not expire.
type Document struct {
	id   string
	Data any           `json:"data"`
	TTL  time.Duration `json:"ttl,omitempty"`
}

// NewDocument builds a Document with its ID already set. Providers use it to
// return stored entries: id is unexported, so it cannot be filled in from a
// struct literal outside this package.
func NewDocument(id string, data any, ttl time.Duration) Document {
	return Document{id: id, Data: data, TTL: ttl}
}

// ID returns the ID of the document. It is empty for a document that has not
// been stored yet and was not given an explicit ID.
func (d *Document) ID() string {
	return d.id
}

// SetID sets the ID of the document. Set one before Create to choose the key
// yourself — a resource name, for instance — instead of having the provider
// generate it.
func (d *Document) SetID(id string) {
	d.id = id
}

// Cache is the contract every provider satisfies.
//
// Every method takes a context: these are network calls, and the caller decides
// how long to wait for them and when to give up.
type Cache interface {
	// Create stores a document with its TTL, assigning an ID when it does not
	// carry one, and returns what was stored.
	Create(ctx context.Context, doc Document) (*Document, error)

	// Get retrieves a document by ID. It returns an error wrapping
	// [ErrNotFound] when no such document exists or it has expired.
	Get(ctx context.Context, id string) (Document, error)

	// Update replaces the content and TTL of an existing document. It returns
	// an error wrapping [ErrNotFound] when the document does not exist.
	Update(ctx context.Context, id string, doc Document) error

	// Delete removes a document and its index entry. Deleting an entry that is
	// not there is not an error — the caller's intent is already satisfied.
	Delete(ctx context.Context, id string) error

	// List returns every stored document, sweeping stale index entries as it
	// goes. Entries that expire between the index read and their fetch are
	// skipped rather than reported as an error.
	List(ctx context.Context) ([]Document, error)
}

// Closer is implemented by providers holding a resource of their own that must
// be released. Providers do not own the client they were handed, so most have
// nothing to close — closing that client stays the caller's job.
type Closer interface {
	Close() error
}
