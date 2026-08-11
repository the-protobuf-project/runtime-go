package options

import "github.com/redis/go-redis/v9"

// re exporting redis types for ease of use
type XMessage = redis.XMessage             // XMessage represents a single message in a Redis stream.
type XReadGroupArgs = redis.XReadGroupArgs // XReadGroupArgs represents the arguments for reading messages from a Redis stream as part of a consumer group.
type XAddArgs = redis.XAddArgs             // XAddArgs represents the arguments for adding a message to a Redis stream.
type Message = redis.Message               // Message represents a message received from a Redis pub/sub channel.
type PubSub = redis.PubSub                 // PubSub represents a Redis pub/sub subscription.
