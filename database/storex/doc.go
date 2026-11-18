// Package storex is the shared runtime the generated GORM stores build on: the
// ListOptions every List/Count call takes, a generic Store interface every
// generated store satisfies, a GenericStore that implements it for any model,
// EnsureSchemas for schema-qualified tables, and the Telemetry seam the
// instrumented stores observe through. Keeping these in one package lets a
// single engine run CRUD for every entity instead of per-entity copies.
//
// It was generated into every project as <go_module>/gormx; the generated
// stores now import it from here, so a fix reaches every project with
// runtime-go rather than with a regeneration.
package storex
