package memcached

import (
	"errors"
	"fmt"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

// Config describes a connection.
type Config struct {
	// Servers are host:port, one per node. A memcached client shards across them
	// on the client side, which is why this is a list and a RESP config's is not
	// — there is no server-side cluster to ask.
	Servers []string

	// Timeout bounds each operation. Zero takes the client's default of 100ms,
	// which is aggressive for anything but a local socket.
	Timeout time.Duration

	// MaxIdleConns caps the idle pool per server. Zero takes the client's
	// default.
	MaxIdleConns int
}

// Client is a connection to memcached that you own.
//
// It wraps the driver so nothing above has to import one, and exposes it again
// through [Client.Unwrap] for the operations this cache does not cover.
type Client struct {
	inner *memcache.Client
}

// NewClient connects to the servers in cfg and verifies one is reachable.
//
// memcached has no PING, so the check is a read of a key that will not be there:
// a cache miss means the server answered, which is all this needs to know. Any
// other error is a server that cannot be reached, and it is better learned here
// than in a request handler.
func NewClient(cfg Config) (*Client, error) {
	if len(cfg.Servers) == 0 {
		return nil, fmt.Errorf("memcached: at least one server is required")
	}
	inner := memcache.New(cfg.Servers...)
	inner.Timeout = cfg.Timeout
	inner.MaxIdleConns = cfg.MaxIdleConns

	if _, err := inner.Get("__runtime_go_probe"); err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
		return nil, fmt.Errorf("memcached: cannot reach %v: %w", cfg.Servers, err)
	}
	return &Client{inner: inner}, nil
}

// Wrap adopts a client you built yourself.
func Wrap(inner *memcache.Client) *Client { return &Client{inner: inner} }

// Unwrap returns the driver client, for anything this package does not do.
func (c *Client) Unwrap() *memcache.Client { return c.inner }

// Close releases the connection.
//
// A memcached client is a pool of sockets with no shutdown of its own, so this
// is a no-op that exists to keep the shape of the backends the same: a caller
// should not have to remember which of them needs closing.
func (c *Client) Close() error { return nil }

// thirtyDays is where memcached stops reading an expiry as a duration and starts
// reading it as an absolute Unix timestamp.
const thirtyDays = 60 * 60 * 24 * 30

// toExpiry converts a Go duration into memcached's expiry.
//
// Past thirty days the protocol switches meaning, so a lease of a year passed
// through unconverted would land in 1970 and the entry would be gone before the
// call returned. A sub-second lease rounds up to one second rather than down to
// zero, because zero means "never expires" here — the one rounding error that
// turns a cache entry into a permanent one.
func toExpiry(ttl time.Duration) int32 {
	if ttl <= 0 {
		return 0
	}
	seconds := int64(ttl.Seconds())
	if seconds < 1 {
		seconds = 1
	}
	if seconds > thirtyDays {
		return int32(time.Now().Add(ttl).Unix())
	}
	return int32(seconds)
}
