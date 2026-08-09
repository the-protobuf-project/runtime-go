package mongodb

// MongoDBSet holds the names of the active database and collection.
type MongoDBSet struct {
	// DatabaseName is the name of the MongoDB database in use.
	DatabaseName string
	// CollectionName is the name of the MongoDB collection in use.
	CollectionName string
}

// MongoDBClientOptions holds the options for connecting to a MongoDB server.
type MongoDBClientOptions struct {
	// DatabaseURL is the base URL for the MongoDB server (e.g. "mongodb://localhost").
	DatabaseURL string
	// Auth contains the credentials for authenticating with MongoDB.
	Auth MongoAuthOptions
}

// MongoAuthOptions holds the username and password for MongoDB authentication.
type MongoAuthOptions struct {
	// Username is the username for authenticating with MongoDB.
	Username string
	// Password is the password for authenticating with MongoDB.
	Password string
}
