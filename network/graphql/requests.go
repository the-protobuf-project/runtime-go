package graphql

// Request builders for operation arguments. These types are shared (not generated per
// resource) because every resource's optional arguments have the same shape: a where filter,
// ordering, and paging for reads; pre/post-check row filters for writes. Sharing them lets a
// generated resource handler satisfy the generic QueryHandler/MutationHandler interfaces, so a
// single adapter can drive CRUD for every entity. Each generated resource package re-exports
// these as type aliases (e.g. `type ListRequest = graphql.ListRequest`) and adds a small
// constructor per operation (List(), Aggregate(), Create(), ...), keeping the fluent calling
// style `resource.List().Where(...).Limit(10)`.
//
// The unexported fields are read by generated handler code through the Get* accessors; a value
// left at its zero is reported unset by IsOmitted and dropped from the operation entirely
// rather than sent as an explicit null.

// ListRequest carries the optional arguments shared by list, find, aggregate, and live-list
// operations: a where filter, result ordering, and limit/offset paging.
type ListRequest struct {
	limit   int
	offset  int
	orderBy []OrderTerm
	where   Predicate
}

// Limit sets the maximum number of rows to return.
func (r *ListRequest) Limit(v int) *ListRequest { r.limit = v; return r }

// Offset sets how many leading rows to skip.
func (r *ListRequest) Offset(v int) *ListRequest { r.offset = v; return r }

// OrderBy sets the result ordering (build terms with a field handle's Asc/Desc).
func (r *ListRequest) OrderBy(v ...OrderTerm) *ListRequest { r.orderBy = v; return r }

// Where sets the row filter predicate.
func (r *ListRequest) Where(v Predicate) *ListRequest { r.where = v; return r }

// GetLimit, GetOffset, GetOrderBy, and GetWhere expose the configured arguments to generated
// handler code, which applies each only when it is set.
func (r *ListRequest) GetLimit() int           { return r.limit }
func (r *ListRequest) GetOffset() int          { return r.offset }
func (r *ListRequest) GetOrderBy() []OrderTerm { return r.orderBy }
func (r *ListRequest) GetWhere() Predicate     { return r.where }

// CreateRequest carries the optional arguments for an insert: a postCheck row filter the
// inserted rows must satisfy (a permission/consistency guard).
type CreateRequest struct{ postCheck Predicate }

// PostCheck sets the post-insert guard predicate.
func (r *CreateRequest) PostCheck(v Predicate) *CreateRequest { r.postCheck = v; return r }

// GetPostCheck exposes the post-insert guard to generated handler code.
func (r *CreateRequest) GetPostCheck() Predicate { return r.postCheck }

// UpdateRequest carries the optional arguments for an update: a preCheck guard (the row must
// match before the write — the basis of optimistic concurrency) and a postCheck guard (the row
// must match after).
type UpdateRequest struct {
	preCheck  Predicate
	postCheck Predicate
}

// PreCheck sets the pre-update guard predicate (e.g. an etag equality).
func (r *UpdateRequest) PreCheck(v Predicate) *UpdateRequest { r.preCheck = v; return r }

// PostCheck sets the post-update guard predicate.
func (r *UpdateRequest) PostCheck(v Predicate) *UpdateRequest { r.postCheck = v; return r }

// GetPreCheck and GetPostCheck expose the guards to generated handler code.
func (r *UpdateRequest) GetPreCheck() Predicate  { return r.preCheck }
func (r *UpdateRequest) GetPostCheck() Predicate { return r.postCheck }

// DeleteRequest carries the optional arguments for a delete: a preCheck guard the row must
// match before removal.
type DeleteRequest struct{ preCheck Predicate }

// PreCheck sets the pre-delete guard predicate.
func (r *DeleteRequest) PreCheck(v Predicate) *DeleteRequest { r.preCheck = v; return r }

// GetPreCheck exposes the pre-delete guard to generated handler code.
func (r *DeleteRequest) GetPreCheck() Predicate { return r.preCheck }
