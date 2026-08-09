package resp

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config describes a connection. The zero value is not usable: an address is
// required, because guessing localhost is how a program ends up caching into
// whatever happens to be listening.
type Config struct {
	// Backend is the name this server reports as — "redis", "dragonfly". It
	// reaches error messages and cache.DB.Backend, and nothing else. Defaults to
	// "redis", since that is what an unnamed RESP server almost always is.
	Backend string

	// Address is host:port.
	Address string

	// Username and Password authenticate, when the server asks.
	Username string
	Password string

	// Database is the index this client is bound to. RESP servers fix it per
	// connection, so this is the one every operation uses unless a caller
	// selects another.
	Database int

	// PoolSize caps concurrent connections. Zero takes the driver's default.
	PoolSize int

	// DialTimeout bounds the initial connection. Zero takes the driver's
	// default.
	DialTimeout time.Duration
}

// Client is a connection to a RESP server that you own.
//
// It wraps the driver so nothing above has to import one, and exposes it again
// through [Client.Unwrap] for the operations this cache does not cover. Hand the
// same client to the database and streams layers and all three share a pool.
type Client struct {
	inner   redis.UniversalClient
	backend string
}

// NewClient dials the server and verifies the connection before returning.
//
// The ping is the point: a client that has not been used yet is
// indistinguishable from a working one, and a cache that only fails on its first
// Get fails in a request handler rather than at startup.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Address == "" {
		return nil, fmt.Errorf("%s: an address is required", name(cfg.Backend))
	}
	inner := redis.NewClient(&redis.Options{
		Addr:        cfg.Address,
		Username:    cfg.Username,
		Password:    cfg.Password,
		DB:          cfg.Database,
		PoolSize:    cfg.PoolSize,
		DialTimeout: cfg.DialTimeout,
	})
	if err := inner.Ping(ctx).Err(); err != nil {
		_ = inner.Close()
		return nil, fmt.Errorf("%s: cannot reach %s: %w", name(cfg.Backend), cfg.Address, err)
	}
	return &Client{inner: inner, backend: name(cfg.Backend)}, nil
}

// Wrap adopts a client you built yourself — a cluster, a ring, a driver
// configured in ways [Config] does not reach. It is not dialed and not pinged
// here, on the grounds that you already have it.
func Wrap(inner redis.UniversalClient, backend string) *Client {
	return &Client{inner: inner, backend: name(backend)}
}

// Unwrap returns the driver client, for anything this package does not do.
func (c *Client) Unwrap() redis.UniversalClient { return c.inner }

// Backend is the name this server reports as.
func (c *Client) Backend() string { return c.backend }

// Close closes the connection pool. Nothing else in this package ever does.
func (c *Client) Close() error {
	if c == nil || c.inner == nil {
		return nil
	}
	return c.inner.Close()
}

// name defaults an empty backend.
func name(backend string) string {
	if backend == "" {
		return "redis"
	}
	return backend
}

// selectDB returns a client bound to index, and a release for it when one had to
// be made.
//
// A RESP server fixes the database when a connection is made — the driver issues
// SELECT as it initializes each pooled connection, and there is no supported way
// to move a live client to another index. So asking for an index other than the
// client's means a second client derived from it: same address, same
// credentials, same pool settings, different DB. The caller closes what it
// derived; the client it was derived from is untouched.
func (c *Client) selectDB(index int) (redis.UniversalClient, func() error, error) {
	switch inner := c.inner.(type) {
	case *redis.Client:
		if inner.Options().DB == index {
			return inner, nil, nil
		}
		opts := *inner.Options() // a copy: the caller's options stay as they were
		opts.DB = index
		derived := redis.NewClient(&opts)
		return derived, derived.Close, nil

	case *redis.ClusterClient, *redis.Ring:
		// Cluster mode has only database 0. Rounding an index down to it would
		// quietly read and write the wrong keys.
		if index != 0 {
			return nil, nil, fmt.Errorf("%s: %T has only database 0, cannot select %d",
				c.backend, inner, index)
		}
		return inner, nil, nil

	default:
		// A client this package has not met — a test fake, a wrapper. It cannot
		// be asked which database it is on, so only 0 can be assumed.
		if index != 0 {
			return nil, nil, fmt.Errorf(
				"%s: cannot select database %d on %T; build the client on that database instead",
				c.backend, index, inner)
		}
		return inner, nil, nil
	}
}
