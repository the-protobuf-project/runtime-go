package options

import "github.com/arangodb/go-driver/v2/arangodb"

type Document struct {
	Key  string                // The unique identifier for the document, typically a ULID.
	Meta arangodb.DocumentMeta // Metadata about the document, including revision and version.
	Data interface{}           // The actual data stored in the document, can be any type.
}
