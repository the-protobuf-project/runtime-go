package mongodb

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Config describes a connection. The zero value is not usable: an address is
// required, because guessing localhost is how a program ends up writing to
// whatever happens to be listening.
type Config struct {
	// URI is the full connection string, e.g. "mongodb://localhost:27017". When
	// set it is used as-is and every other field here is ignored — a URI is the
	// one form that can express a replica set, a read preference, TLS and an
	// authSource without this struct growing a field for each.
	URI string

	// Address is host:port, used when URI is empty.
	Address string

	// Username and Password authenticate, when the server asks.
	//
	// They are escaped into the URI rather than concatenated. A password
	// containing @ or / or : is ordinary, and pasting one into a connection
	// string unescaped either fails to parse or silently connects somewhere
	// else — the host is whatever follows the last @.
	Username string
	Password string

	// AuthSource is the database credentials are checked against. Empty leaves
	// it to the driver, which uses "admin" for a URI that names no store.
	AuthSource string

	// ConnectTimeout bounds both the dial and server selection. Zero takes the
	// driver's defaults, where selection alone is thirty seconds — set this in
	// anything that checks its dependencies at startup.
	ConnectTimeout time.Duration
}

// Client is a connection to MongoDB that you own.
//
// It wraps the driver so nothing above has to import one, and exposes it again
// through [Client.Unwrap] for the operations this package does not cover.
type Client struct {
	inner *mongo.Client
}

// NewClient dials MongoDB and verifies the connection before returning.
//
// The ping is the point: a client that has not been used yet is
// indistinguishable from a working one, and a store that only fails on its
// first read fails in a request handler rather than at startup.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	uri, err := cfg.uri()
	if err != nil {
		return nil, err
	}

	opts := options.Client().ApplyURI(uri)
	if cfg.ConnectTimeout > 0 {
		// Both, because they bound different things and only setting the first
		// leaves an unreachable address hanging for the driver's 30-second
		// server-selection default — long enough that a startup check looks
		// like a hang rather than a failure.
		opts = opts.SetConnectTimeout(cfg.ConnectTimeout).
			SetServerSelectionTimeout(cfg.ConnectTimeout)
	}

	inner, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("mongodb: cannot connect: %w", err)
	}
	if perr := inner.Ping(ctx, nil); perr != nil {
		_ = inner.Disconnect(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("mongodb: cannot reach the server: %w", perr)
	}
	return &Client{inner: inner}, nil
}

// Wrap adopts a client you built yourself — one configured in ways [Config] does
// not reach. It is not dialed and not pinged here, on the grounds that you
// already have it.
func Wrap(inner *mongo.Client) *Client { return &Client{inner: inner} }

// Unwrap returns the driver client, for anything this package does not do.
func (c *Client) Unwrap() *mongo.Client { return c.inner }

// Close disconnects the client. Nothing else in this package ever does.
func (c *Client) Close(ctx context.Context) error {
	if c == nil || c.inner == nil {
		return nil
	}
	return c.inner.Disconnect(ctx)
}

// uri builds the connection string, escaping credentials.
func (c Config) uri() (string, error) {
	if c.URI != "" {
		return c.URI, nil
	}
	if c.Address == "" {
		return "", fmt.Errorf("mongodb: an address or a URI is required")
	}

	host := strings.TrimPrefix(c.Address, "mongodb://")
	u := &url.URL{Scheme: "mongodb", Host: host}
	if c.Username != "" {
		// url.UserPassword escapes both, so a password containing @ or / ends
		// up meaning itself rather than changing which host is dialed.
		u.User = url.UserPassword(c.Username, c.Password)
	}
	if c.AuthSource != "" {
		q := u.Query()
		q.Set("authSource", c.AuthSource)
		u.RawQuery = q.Encode()
	}
	return u.String(), nil
}
