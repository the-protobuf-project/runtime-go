# Reference

The ArangoDB and MongoDB clients written before this module was restructured,
kept for what they encode about each backend rather than to be compiled.

The directory name begins with an underscore, so the Go tool ignores it: the
code imports `github.com/machanirobotics/loom/...`, which no longer resolves,
and it would otherwise break the build. Their `go.mod` files have been removed —
they existed only to keep this code out of the parent module, which the
underscore now does.

What was taken from it, and what was deliberately not:

- **Kept**: the ArangoDB graph model — a named graph over edge definitions, each
  declaring which vertex collections an edge may join. `database.EdgeDefinition`
  is that idea, generalized so Neo4j fits it too.
- **Kept**: the manager-per-concern shape (`Graph` / `Collection` / `Document`),
  which is what `database.DB` does with its capability fields.
- **Changed**: the managers held mutable `DatabaseName` / `CollectionName` on the
  client, so two collections in one process aliased each other. Selection now
  returns a value rather than mutating one.
- **Changed**: `NewMongoDBClient` concatenated credentials into the URI, so a
  password containing `@` chose a different host. Credentials are escaped.
- **Changed**: `NewArangoDBClient` hardcoded basic auth as root/root and called
  `log.Fatalf` from library code.
- **Changed**: `SetDatabase` returned a manager whose `Document` field was nil.
  Every capability field is now non-nil and refuses by name.
