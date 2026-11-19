// Package osx is the runtime the generated OpenSearch indices build on: a
// (store.v1.projection) copied from its Postgres table into OpenSearch and
// searched there. A generated index is an Index[T] value holding the
// projection and its mapping; everything it does is here, once.
//
// The Postgres table stays the source of truth. Its rows reach OpenSearch
// through the repositories' change feed (repox.Projections.Feed, served by an
// Indexer) as bulk writes with version_type=external, the version being the
// row's row_version, so a late or repeated change never overwrites a newer
// one; Reindex rebuilds the copy from the table behind an alias swap.
//
// Searches take the same repox.SearchInput as the Postgres search: an
// AIP-160 filter translated by the filterx OpenSearch engine (JSON attribute
// paths included), the time window as a date_range "contains" query, an
// AIP-132 order_by, and page tokens carrying the search_after values of the
// page's last hit (and, when Index.PageKeepAlive asks for one, a point in
// time).
package osx
