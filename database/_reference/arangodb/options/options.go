// Package options provides configuration options for the ArangoDB client.
package options

// ArangoDBClientOptions defines the options for creating a new ArangoDB client.
// It includes the endpoints, database name, collection name, user credentials, and connection timeout.
// This struct is used to configure the ArangoDB client during initialization.
type ArangoDBClientOptions struct {
	Endpoints         []string // List of ArangoDB endpoints to connect to.
	Credentials       string   // Credentials for authentication in the format "username:password".
	DatabaseName      string   // Name of the database to connect to.
	CollectionName    string   // Name of the collection to operate on.
	ConnectionTimeout uint     // Timeout for the connection in seconds.
}
