// Package arangodb will implement the [database.Store] contract over arangodb.
//
// It is not implemented yet. When it lands it will follow the same shape as
// the redis provider: a typed Config carrying an already-built client —
// roughly Config{Client arangodb.Client, Database, Collection string} — and a New constructor that
// validates it, so this package never opens or closes a connection of its own.
package arangodb
