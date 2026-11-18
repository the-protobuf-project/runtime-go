// Package filterx is the filter/order/list SDK the generated specs build on: the Condition
// vocabulary an AIP-160 filter parses into, the Spec data model the per-schema
// filters.go files instantiate, the shared semantics (enum normalization,
// date/number validation, ILIKE escaping, sort allowlists, page math), and the
// chainable engines built from a Spec — Gorm[M] for SQL, Hasura[M] for
// GraphQL, and OpenSearch for the query DSL. Engines are code, specs are data:
// one Spec per table drives every backend, so all accept identical filter
// strings by construction.
//
// It was generated into every project as <go_module>/filterx; the generated
// filter specs now import it from here, so a fix reaches every project with
// runtime-go rather than with a regeneration.
package filterx
