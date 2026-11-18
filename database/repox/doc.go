// Package repox is the shared runtime of the generated repositories: the error
// vocabulary adapters map their backend errors onto, the connection bundle the
// per-schema factories pick an adapter from, the list-request shape every
// repository List takes, the Unit every schema's Batch queues into, and the
// small helpers (resource-name splitting, ULID minting, mask matching) the
// generated method bodies share.
//
// It was generated into every project as <go_module>/repox; the generated
// repositories now import it from here, so a fix reaches every project with
// runtime-go rather than with a regeneration. Conn.GraphQL is the one handle
// the generated code supplies: the project's GraphQL client Service, which
// satisfies GraphQL. The generated code reads its typed queries back with a
// type assertion; this package only opens transactions on it.
package repox
