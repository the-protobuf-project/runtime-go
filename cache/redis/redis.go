package redis

import (
	"context"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/cache/resp"
)

// Backend is the name a cache built here reports.
const Backend = "redis"

// DefaultAddress is where a Redis server listens unless told otherwise.
const DefaultAddress = "localhost:6379"

// Config describes a connection. It is [resp.Config] with the backend already
// chosen, so the field is not yours to set.
type Config struct {
	// Address is host:port. Empty means [DefaultAddress].
	Address string

	// Username and Password authenticate, when the server asks.
	Username string
	Password string

	// Database is the index this client is bound to.
	Database int

	// PoolSize caps concurrent connections. Zero takes the driver's default.
	PoolSize int

	// DialTimeout bounds the initial connection. Zero takes the driver's
	// default.
	DialTimeout time.Duration
}

// Client is a connection to Redis. It is [resp.Client] under another name, so a
// client built here can be handed to anything expecting one.
type Client = resp.Client

// NewClient dials Redis and verifies the connection before returning.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	address := cfg.Address
	if address == "" {
		address = DefaultAddress
	}
	return resp.NewClient(ctx, resp.Config{
		Backend:     Backend,
		Address:     address,
		Username:    cfg.Username,
		Password:    cfg.Password,
		Database:    cfg.Database,
		PoolSize:    cfg.PoolSize,
		DialTimeout: cfg.DialTimeout,
	})
}

// Adopt takes a driver client you built yourself — a cluster, a ring, a
// configuration [Config] does not reach — and presents it as a Redis client.
func Adopt(client *Client) *Client { return client }

// New returns a cache backed by a client you own.
func New(client *Client, cfg cache.Config) cache.Provider {
	return resp.New(client, cfg)
}
