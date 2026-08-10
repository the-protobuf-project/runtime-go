package options

// CollectionSchemaOptions defines the schema for a collection in ArangoDB.
type CollectionSchemaOptions struct {
	Description string                 // Description of the collection schema.
	Schema      map[string]interface{} // Schema rules for the collection.
	Level       CollectionSchemaLevel  // Level of schema enforcement.
}

// CollectionSchemaLevel defines the level of schema enforcement for a collection.
type CollectionSchemaLevel string

const (
	CollectionSchemaLevelNone     CollectionSchemaLevel = "none"     // No schema enforcement.
	CollectionSchemaLevelNew      CollectionSchemaLevel = "new"      // New documents must conform to the schema.
	CollectionSchemaLevelModerate CollectionSchemaLevel = "moderate" // Moderate schema enforcement, allowing some flexibility.
	CollectionSchemaLevelStrict   CollectionSchemaLevel = "strict"   // Strict schema enforcement, all documents must conform to the schema.
)

// CollectionType is the type of a collection.
type CollectionType int

const (
	// CollectionTypeDocument specifies a document collection
	CollectionTypeDocument = CollectionType(2)
	// CollectionTypeEdge specifies an edges collection
	CollectionTypeEdge = CollectionType(3)
)
