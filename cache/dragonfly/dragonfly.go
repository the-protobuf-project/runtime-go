package dragonfly

import (
	"context"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/cache/resp"
)

// Backend is the name a cache built here reports.
const Backend = "dragonfly"

// DefaultAddress is where Dragonfly listens unless told otherwise. It takes
// Redis' port deliberately, being a drop-in for it.
const DefaultAddress = "localhost:6379"

// DefaultDatabases is how many databases Dragonfly serves unless started with
// another --dbnum. Selecting one at or past it is refused by the server.
const DefaultDatabases = 16

// Config describes a connection.
type Config struct {
	// Address is host:port. Empty means [DefaultAddress].
	Address string

	// Username and Password authenticate, when the server asks.
	Username string
	Password string

	// Database is the index this client is bound to. Dragonfly serves
	// [DefaultDatabases] of them unless started with another --dbnum.
	Database int

	// PoolSize caps concurrent connections. Zero takes the driver's default.
	PoolSize int

	// DialTimeout bounds the initial connection. Zero takes the driver's
	// default.
	DialTimeout time.Duration
}

// Client is a connection to Dragonfly. It is [resp.Client] under another name,
// so a client built here can be handed to anything expecting one.
type Client = resp.Client

// NewClient dials Dragonfly and verifies the connection before returning.
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

// New returns a cache backed by a client you own.
func New(client *Client, cfg cache.Config) cache.Provider {
	return resp.New(client, cfg)
}
