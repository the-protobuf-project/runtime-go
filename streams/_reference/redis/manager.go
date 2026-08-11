package redis

// RedisDBManager provides a high-level interface for interacting with Redis,
// exposing grouped handlers for different logical data components such as
// documents and channels. It acts as the root entry point for all Redis-backed
// operations within the Loom package.
type RedisDBManager struct {
	client   *Client
	Document *documentHandler
	Channel  *channelHandler
}

// DatabaseManager defines the operations for managing Redis logical databases.
// It provides methods for creating, retrieving, selecting, deleting, and listing
// named databases, each of which is internally mapped to a Redis DB index.
type DatabaseManager interface {
	// CreateDatabase creates a new database with the given name. If the name
	// already exists, an error is returned. On success, it returns a
	// RedisDBManager configured to operate on the newly created database.
	CreateDatabase(name string) error

	// GetDatabase returns the Redis database ID mapped to the given name.
	// If the database does not exist, an error is returned.
	GetDatabase(name string) (map[string]int, error)

	// SetDatabase selects the existing database identified by the given name
	// and returns a RedisDBManager instance configured to operate on it.
	// If the database does not exist, an error is returned.
	SetDatabase(name string) (*RedisDBManager, error)

	// DeleteDatabase removes the database mapping for the given name and
	// flushes all data from the underlying Redis DB. Attempting to delete DB 0
	// results in an error.
	DeleteDatabase(name string) error

	// ListDatabases returns a map of all registered database names and their
	// corresponding Redis DB IDs.
	ListDatabases() (map[string]int, error)
}

// channelHandler is an alias for RedisChannelManager to satisfy existing references.
type channelHandler struct {
	Stream *StreamHandler
	Notify *NotifyHandler
}

// GetChannelHandler returns a new instance of RedisChannelManager.
// Which has Stream and Notify handlers
func GetChannelHandler(client *Client) *channelHandler {
	return &channelHandler{
		Stream: &StreamHandler{Client: client},
		Notify: &NotifyHandler{client: client},
	}
}

// RedisChannelManager manages Redis-backed communication channels.
// It provides access to both publishing and subscribing interfaces.
type RedisStreamManager struct {
	Publisher  StreamPublisher  // Publisher ChannelPublisher
	Subscriber StreamSubscriber // Subscriber ChannelSubscriber
}
