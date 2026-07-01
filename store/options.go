package store

import "google.golang.org/protobuf/proto"

// ListOptions controls pagination, ordering, and filtering for List and Count.
// It is shaped after AIP-132 (page_size / page_token) rather than SQL
// limit/offset, so the same options translate to a relational query, an
// eth_call range, or a subgraph GraphQL query. A driver that cannot honor a
// field (e.g. an on-chain driver that delegates filtering to a subgraph) is
// free to ignore it.
type ListOptions struct {
	// PageSize caps the number of records returned; <= 0 means the driver default.
	PageSize int32

	// PageToken is an opaque continuation token from a previous List response.
	// Drivers encode whatever they need into it; the adapter passes it through.
	PageToken string

	// OrderBy is an AIP-132 order expression, e.g. "title desc". Optional.
	OrderBy string

	// Filter is an AIP-160 filter expression. Optional; backends that lack
	// server-side filtering ignore it.
	Filter string
}

// ListResult is what a Driver.List returns: the page of records plus the
// AIP-132 continuation token and total size. NextPageToken is empty on the last
// page; Total is the count of records matching the filter ignoring pagination,
// or -1 when a backend cannot compute it cheaply.
type ListResult struct {
	Items         []proto.Message
	NextPageToken string
	Total         int64
}

// WriteResult is returned by Create and Update. For a synchronous backend (a SQL
// database) Pending is false and Message holds the persisted record. For an
// asynchronous backend (a blockchain) Pending is true: the write was submitted
// but not finalized, Message holds the optimistic value, and Metadata carries
// backend handles (e.g. {"txHash": "0x…", "operation": "operations/…"}) that the
// adapter surfaces as a long-running operation rather than pretending the write
// completed synchronously.
type WriteResult struct {
	// Message is the resulting record. For a pending write it is the submitted
	// value, not a confirmed read-back.
	Message proto.Message

	// Pending is true when the backend has accepted but not finalized the write.
	Pending bool

	// Metadata carries backend-specific handles for a pending write. Nil for a
	// synchronous result.
	Metadata map[string]string
}
