package arangodb

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/arangodb/go-driver/v2/arangodb"
	"github.com/arangodb/go-driver/v2/connection"
)

// Config describes a connection. The zero value is not usable: an endpoint is
// required, because guessing localhost is how a program ends up writing to
// whatever happens to be listening.
type Config struct {
	// Endpoints are the servers to reach, e.g. "http://localhost:8529". More
	// than one is round-robined, which is how a coordinator set is addressed.
	Endpoints []string

	// Username and Password authenticate.
	//
	// They are used as given. The reference client this replaced hardcoded
	// root/root and ignored what it was passed, which meant a deployment with
	// real credentials silently connected as root or not at all.
	Username string
	Password string

	// ConnectTimeout bounds a request. Zero takes the driver's default.
	ConnectTimeout time.Duration

	// MaxConnsPerHost caps concurrent connections to one coordinator. Zero takes
	// Go's default of unlimited, which is rarely what anyone wants: it lets a
	// burst open as many sockets as there are in-flight requests, and the server
	// runs out of file descriptors before the client notices anything is wrong.
	MaxConnsPerHost int

	// MaxIdleConnsPerHost keeps connections open between requests. Zero takes
	// Go's default of two, which is far too few for a service under load — every
	// request past the second pays a fresh TCP and TLS handshake, and that cost
	// is invisible in a benchmark that runs one request at a time.
	MaxIdleConnsPerHost int

	// IdleConnTimeout closes a connection idle for longer than this. Zero takes
	// Go's default.
	IdleConnTimeout time.Duration
}

// Client is a connection to ArangoDB that you own.
//
// It wraps the driver so nothing above has to import one, and exposes it again
// through [Client.Unwrap] for the operations this package does not cover — an
// AQL query of your own, most likely.
type Client struct {
	inner arangodb.Client
}

// NewClient dials ArangoDB and verifies the connection before returning.
//
// The version call is the point: a client that has not been used yet is
// indistinguishable from a working one, and a store that only fails on its
// first read fails in a request handler rather than at startup.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("arangodb: at least one endpoint is required")
	}

	// The transport is built here rather than left to the driver, because its
	// defaults are Go's: unlimited connections per host and two idle ones. Both
	// are wrong for a service under load, in opposite directions — the first
	// lets a burst open as many sockets as there are in-flight requests until
	// the server runs out of descriptors, the second makes every request past
	// the second pay a fresh handshake.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.MaxConnsPerHost > 0 {
		transport.MaxConnsPerHost = cfg.MaxConnsPerHost
	}
	if cfg.MaxIdleConnsPerHost > 0 {
		transport.MaxIdleConnsPerHost = cfg.MaxIdleConnsPerHost
	}
	if cfg.IdleConnTimeout > 0 {
		transport.IdleConnTimeout = cfg.IdleConnTimeout
	}

	conn := connection.NewHttpConnection(connection.HttpConfiguration{
		Endpoint:    connection.NewRoundRobinEndpoints(cfg.Endpoints),
		ContentType: connection.ApplicationJSON,
		Transport:   transport,
	})
	if cfg.Username != "" || cfg.Password != "" {
		if err := conn.SetAuthentication(connection.NewBasicAuth(cfg.Username, cfg.Password)); err != nil {
			return nil, fmt.Errorf("arangodb: cannot set authentication: %w", err)
		}
	}

	inner := arangodb.NewClient(conn)

	probe := ctx
	if cfg.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		probe, cancel = context.WithTimeout(ctx, cfg.ConnectTimeout)
		defer cancel()
	}
	if _, err := inner.Version(probe); err != nil {
		return nil, fmt.Errorf("arangodb: cannot reach %v: %w", cfg.Endpoints, err)
	}
	return &Client{inner: inner}, nil
}

// Wrap adopts a client you built yourself — one configured in ways [Config]
// does not reach. It is not dialed and not checked here, on the grounds that
// you already have it.
func Wrap(inner arangodb.Client) *Client { return &Client{inner: inner} }

// Unwrap returns the driver client, for anything this package does not do.
func (c *Client) Unwrap() arangodb.Client { return c.inner }

// Close releases the connection.
//
// The driver holds an HTTP client with no shutdown of its own, so this is a
// no-op that exists to keep the shape of the backends the same: a caller should
// not have to remember which of them needs closing.
func (c *Client) Close() error { return nil }
