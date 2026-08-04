// Package database defines the backend-agnostic contract for runtime-go's
// document-store layer: durable records that live until they are deleted.
//
// Providers live in subpackages — [github.com/the-protobuf-project/runtime-go/database/redis]
// and friends — and each exposes a typed Config plus a New constructor:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 0})
//	db, err := dbredis.New(dbredis.Config{Client: rdb, Prefix: "orders"})
//
// A provider never opens its own connection. The caller builds the client, so
// its lifetime, pooling, and shutdown stay the caller's to control and this
// package holds no process-wide connection state.
//
// # Not a cache
//
// Records here have no TTL and do not expire. For ephemeral, TTL-bound entries
// use the sibling [github.com/the-protobuf-project/runtime-go/cache] module.
// The two are separate because they answer different questions: a cache miss is
// routine, a missing database record is not.
//
// # Not the proto Driver
//
// This is a store for ad-hoc JSON documents. The generated-proto CRUD seam —
// [github.com/the-protobuf-project/runtime-go/interfaces/store] — operates on
// proto.Message values through Resource descriptors and serves a different job.
// Both exist on purpose; forcing arbitrary documents through proto descriptors
// would be lossy in one direction and pointless in the other.
package database

import (
	"context"
	"errors"
)

var (
	// ErrNotFound is returned when a document does not exist. Providers wrap it
	// with the ID for context, so callers test with errors.Is rather than
	// comparing formatted error strings.
	ErrNotFound = errors.New("database: not found")

	// ErrDuplicate is returned when a write would store content that already
	// exists under a different ID.
	//
	// Providers that deduplicate by content hash report this instead of
	// silently creating a second copy — see [Store.Create].
	ErrDuplicate = errors.New("database: duplicate content")
)

// Document is a stored record: a JSON-serializable payload under an ID.
//
// Unlike its cache counterpart there is no TTL — a document lives until it is
// deleted.
type Document struct {
	id   string
	Data any `json:"data"`
}

// NewDocument builds a Document with its ID already set. Providers use it to
// return stored records: id is unexported, so it cannot be filled in from a
// struct literal outside this package.
func NewDocument(id string, data any) Document {
	return Document{id: id, Data: data}
}

// ID returns the ID of the document.
func (d *Document) ID() string {
	return d.id
}

// SetID sets the ID of the document. Set one before Create to choose the key
// yourself instead of having the provider generate it.
func (d *Document) SetID(id string) {
	d.id = id
}

// Query narrows a [Store.List]. The zero Query returns everything.
type Query struct {
	// Limit caps how many documents are returned. Zero means no limit.
	Limit int

	// Offset skips this many documents before collecting results. It is only
	// meaningful alongside a stable order, which providers guarantee by sorting
	// IDs — see [Store.List].
	Offset int
}

// Store is the contract every provider satisfies.
//
// Every method takes a context: these are network calls, and the caller decides
// how long to wait and when to give up.
type Store interface {
	// Create stores a document, assigning an ID when it does not carry one, and
	// returns what was stored.
	//
	// Providers that deduplicate by content return the existing document rather
	// than storing a second copy of identical content — check the returned ID
	// against the one you supplied to tell the two apart.
	Create(ctx context.Context, doc Document) (*Document, error)

	// Get retrieves a document by ID. It returns an error wrapping
	// [ErrNotFound] when no such document exists.
	Get(ctx context.Context, id string) (Document, error)

	// Update replaces the content of an existing document. It returns an error
	// wrapping [ErrNotFound] when the document does not exist, and one wrapping
	// [ErrDuplicate] when the new content already belongs to a different
	// document.
	Update(ctx context.Context, id string, doc Document) error

	// Delete removes a document and its indexes. It returns an error wrapping
	// [ErrNotFound] when the document does not exist — unlike a cache, a
	// missing record here is a genuine surprise worth reporting.
	Delete(ctx context.Context, id string) error

	// List returns stored documents in a stable order, narrowed by q.
	List(ctx context.Context, q Query) ([]Document, error)
}

// Closer is implemented by providers holding a resource of their own that must
// be released. Providers do not own the client they were handed, so most have
// nothing to close — closing that client stays the caller's job.
type Closer interface {
	Close() error
}
