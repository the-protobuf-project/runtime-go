package store

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// Driver is the CRUD contract every storage backend implements. It is the single
// seam the gRPC adapter depends on, so swapping a relational database for an
// EVM chain or Hyperledger Fabric never touches the proto API or the adapter.
//
// All methods operate on proto.Message directly (read and written through the
// [Resource] descriptor via protoreflect), so a backend needs no generated Go
// model types. Read methods (Get/List/Count/Exists) are expected to be
// synchronous; write methods (Create/Update) return a [WriteResult] whose
// Pending flag lets an asynchronous backend report a submitted-but-not-finalized
// write instead of blocking or lying about completion.
//
// Drivers signal storage outcomes with the package sentinel errors
// ([ErrNotFound], [ErrAlreadyExists], [ErrUnimplemented]) so the adapter can map
// them to gRPC status codes.
type Driver interface {
	// Create stores msg as a new record, returning ErrAlreadyExists if a record
	// with the same key already exists.
	Create(ctx context.Context, res *Resource, msg proto.Message) (WriteResult, error)

	// Get returns the record whose primary key equals key, or ErrNotFound.
	Get(ctx context.Context, res *Resource, key string) (proto.Message, error)

	// Update overwrites the record identified by msg's primary key, returning
	// ErrNotFound if it does not exist.
	Update(ctx context.Context, res *Resource, msg proto.Message) (WriteResult, error)

	// Delete removes the record whose primary key equals key, returning
	// ErrNotFound if it does not exist.
	Delete(ctx context.Context, res *Resource, key string) error

	// List returns the page of records matching opts, the continuation token, and
	// the total size (see ListResult). The driver owns page-token encoding; the
	// adapter passes opts.PageToken through and returns ListResult.NextPageToken.
	List(ctx context.Context, res *Resource, opts ListOptions) (ListResult, error)

	// Count returns the number of records matching opts.Filter.
	Count(ctx context.Context, res *Resource, opts ListOptions) (int64, error)

	// Exists reports whether a record with the given primary key exists.
	Exists(ctx context.Context, res *Resource, key string) (bool, error)
}
