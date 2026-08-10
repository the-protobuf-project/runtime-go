package database

import "github.com/the-protobuf-project/runtime-go/database/store"

// ListOption narrows or pages a listing.
//
// Options rather than a struct so a call says only what it means: a listing that
// wants twenty of something reads as Page(20) rather than as a literal with four
// zero fields, and a backend can gain a way to narrow a read without changing
// any signature.
type ListOption func(*store.ListOptions)

// Page caps how many records come back.
func Page(n int) ListOption {
	return func(o *store.ListOptions) { o.PageSize = int32(n) }
}

// Where narrows a listing with an AIP-160 expression.
//
// Every backend here accepts conjunctions of `column op value` with
// = != > >= < <= and refuses anything else by name. That is a small fraction of
// the grammar, and the smallness is deliberate: a store that accepted the whole
// thing and honored part of it would return the wrong records with nothing to
// say it had ignored something.
//
//	books.List(ctx, database.Where("published_year >= 1984"), database.Page(20))
func Where(filter string) ListOption {
	return func(o *store.ListOptions) { o.Filter = filter }
}

// OrderBy sorts a listing, e.g. "published_year desc".
func OrderBy(expr string) ListOption {
	return func(o *store.ListOptions) { o.OrderBy = expr }
}

// pageToken continues a listing from where the last page ended. It is unexported
// because the token is opaque by contract — a caller passes back what List
// handed it, which [Coll.All] does on its behalf.
func pageToken(token string) ListOption {
	return func(o *store.ListOptions) { o.PageToken = token }
}

// listOptions folds the options into the shape the layer underneath takes.
func listOptions(opts []ListOption) store.ListOptions {
	var o store.ListOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}
