// package options provides data structures for application configuration
// and for handling generic data payloads from sources like Redis.
package options

// RedisClientOptions holds all the necessary parameters to establish a
// connection with a Redis server.
type RedisClientOptions struct {
	// Address is the Redis server address in the format "locahost"
	Address string
	// Port is the Redis server port. If not specified, defaults to "6379".
	Port string
	// Username is the Redis server username for authentication.
	Username string
	// Password is the Redis server password for authentication.
	Password string
	// Database is the Redis database number to connect to. Defaults to "0".
	Database string
}
