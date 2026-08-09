package options

import "github.com/arangodb/go-driver/v2/arangodb"

type DatabaseInfo = arangodb.DatabaseInfo // DatabaseInfo provides metadata about an ArangoDB database.
type Database = arangodb.Database         // this is an interface, not a struct
type CreateCollectionOptions = arangodb.CreateCollectionOptions
type CollectionProperties = arangodb.CollectionProperties // CollectionProperties provides metadata about an ArangoDB collection.
