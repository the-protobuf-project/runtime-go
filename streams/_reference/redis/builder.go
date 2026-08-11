package redis

import (
	"net"
	"strconv"

	"github.com/machanirobotics/loom/go/redis/options"
	"github.com/machanirobotics/loom/go/redis/shared"
)

// buildClient constructs a new client bound to the database identified
// by the given id. It initializes a new Redis client configured with the appropriate DB index.
// The returned RedisDBManager exposes Document and Channel managers that operate
// within the selected logical database.
func (d *Client) buildClient(dbId int, opts ...options.RedisClientOptions) (*Client, error) {
	shared.Pulse.Logger.Debugf("Building client for database %d with options %v", dbId, opts)
	if d.redisClient == nil {
		return nil, shared.Pulse.Logger.Error("Redis not Initialised")
	}

	var redisOpts options.RedisClientOptions
	host, _, err := net.SplitHostPort(d.redisClient.Options().Addr)

	if err != nil {
		return nil, shared.Pulse.Logger.Errorf("Error spliting the url:%s", err)
	}
	if len(opts) > 0 {
		redisOpts = opts[0]
	}

	// Always override the database ID with the provided one
	redisOpts.Address = host
	redisOpts.Username = d.redisClient.Options().Username
	redisOpts.Password = d.redisClient.Options().Password

	redisOpts.Database = strconv.Itoa(dbId) // This will override any previous database setting

	newClient, err := newClient(redisOpts)
	if err != nil {
		return nil, shared.Pulse.Logger.Errorf("Error inside build client%v", err)
	}
	// Verify the client is connected to the correct database
	_, _ = strconv.Atoi(redisOpts.Database)

	shared.Pulse.Logger.Debugf("Client built for database %d", dbId)
	clientInstance = newClient

	return newClient, nil
}
