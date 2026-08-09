package mongodb

// Compile-time interface compliance assertions.
//
// These assignments ensure that MongoDBClient implements the
// required operation interfaces. If MongoDBClient fails to satisfy
// any of these interfaces, the compiler will generate an error.
var (
	// Assert MongoDBClient implements DatabaseManager.
	_ DatabaseManager = (*MongoDBClient)(nil)
	// Assert MongoDBClient implements CollectionManager.
	_ CollectionManager = (*MongoDBClient)(nil)
	// Assert MongoDBClient implements DocumentWriter.
	_ DocumentWriter = (*MongoDBClient)(nil)
	// Assert MongoDBClient implements DataImporterExporter.
	_ DataImporterExporter = (*MongoDBClient)(nil)
)
