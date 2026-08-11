package redis

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/machanirobotics/loom/go/redis/options"
	"github.com/machanirobotics/loom/go/redis/shared"
	"github.com/redis/go-redis/v9"
)

var (
	// clientInstance is the singleton instance of the Redis client
	clientInstance *Client
	// clientOnce ensures the client is initialized only once
	clientMu  sync.Mutex
	currentDB int
)

// Client wraps a Redis client and provides additional functionality
// such as telemetry integration via Pulse.
type Client struct {
	redisClient *redis.Client
}

func newClient(opts ...options.RedisClientOptions) (*Client, error) {
	clientMu.Lock()
	defer clientMu.Unlock()

	redisOpts, err := validateOptions(opts...)

	if err != nil {
		return nil, fmt.Errorf("failed to validate options: %w", err)
	}

	dbNum, err := strconv.Atoi(redisOpts.Database)

	if err != nil {
		return nil, fmt.Errorf("invalid database number: %w", err)
	}

	addr := fmt.Sprintf("%s:%s", redisOpts.Address, redisOpts.Port)

	// If client already exists and DB is SAME → reuse instance
	if clientInstance != nil && dbNum == currentDB {
		shared.Pulse.Logger.Debugf("Reusing existing Redis client (DB: %d)", currentDB)
		return clientInstance, nil
	}

	// If switching DB → create a new client without closing the old one
	// The old client will be garbage collected when no longer referenced
	if clientInstance != nil && dbNum != currentDB {
		shared.Pulse.Logger.Debugf(
			"Creating new Redis client (switching from DB %d to %d)",
			currentDB, dbNum,
		)
	}

	// Create new client
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Username: redisOpts.Username,
		Password: redisOpts.Password,
		DB:       dbNum,
	})

	// Test connection
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis DB %d: %w", dbNum, err)
	}

	clientInstance = &Client{
		redisClient: rdb,
	}
	currentDB = dbNum

	shared.Pulse.Logger.Debugf(
		"Redis client initialized successfully (DB: %d, addr: %s)",
		dbNum, addr,
	)
	return clientInstance, nil
}

func NewRedisClient(opts ...options.RedisClientOptions) (DatabaseManager, error) {
	client, err := newClient(opts...)
	if err != nil {
		return nil, err
	}
	shared.Pulse.Logger.Infof("Redis client initialized successfully (DB: %d, addr: %s)", client.redisClient.Options().DB, client.redisClient.Options().Addr)
	return DatabaseManager(client), nil
}

// Close shuts down the Redis client and closes Pulse telemetry. Returns
// an error if closing the client fails
func (c *Client) Close() error {
	if c.redisClient != nil {
		err := c.redisClient.Close()
		if err != nil {
			_ = shared.Pulse.Logger.Errorf("Error closing Redis client: %v", err)
			return err
		}
		shared.Pulse.Logger.Info("Redis client closed successfully")
	}
	err := shared.Close()
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("Error closing Pulse telemetry: %v", err)
		return err
	}
	return nil
}
