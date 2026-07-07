package graphql

import "context"

// QueryHandler and MutationHandler are the generic CRUD surfaces that every generated resource
// handler satisfies (alongside its resource-specific methods such as Aggregate). They let a
// generic adapter — e.g. a Hasura engine — drive reads and writes for any entity through one
// pair of interfaces instead of a hand-written, copy-pasted handler per entity. Each generated
// resource package emits a compile-time `var _ QueryHandler[Row] = (*queryHandler)(nil)`
// assertion (and the mutation equivalent) so this contract is verified at build time.

// QueryHandler is the generic read surface for a resource whose row model is M. The request
// argument is the shared ListRequest, so the adapter builds one request and passes it to any
// entity's handler.
type QueryHandler[M any] interface {
	// Get returns the row with the given id, or nil when absent.
	Get(ctx context.Context, id string) (*M, error)
	// List returns the rows matching the request.
	List(ctx context.Context, req ...*ListRequest) ([]M, error)
	// Find returns the first row matching the request, or nil when none match.
	Find(ctx context.Context, req ...*ListRequest) (*M, error)
}

// MutationHandler is the generic write surface for a resource. C is the create input and U the
// update patch; IR, UR, and DR are the insert, update, and delete response models (an engine
// like Hasura returns a distinct mutation_response type per verb, so they are separate type
// parameters rather than one shared R).
type MutationHandler[C, U, IR, UR, DR any] interface {
	// Create inserts obj and returns the insert response.
	Create(ctx context.Context, obj C, req ...*CreateRequest) (IR, error)
	// Update applies patch to the row with the given id and returns the update response.
	Update(ctx context.Context, id string, patch U, req ...*UpdateRequest) (UR, error)
	// Delete removes the row with the given id and returns the delete response.
	Delete(ctx context.Context, id string, req ...*DeleteRequest) (DR, error)
}
