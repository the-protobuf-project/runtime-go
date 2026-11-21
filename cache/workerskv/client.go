package workerskv

import (
	"context"
	"fmt"

	"github.com/cloudflare/cloudflare-go/v7"
	"github.com/cloudflare/cloudflare-go/v7/kv"
)

// Config says which namespace a client reads and writes, and through which SDK
// client.
type Config struct {
	// Client is the SDK client, carrying the API token. It is yours: this
	// package neither builds nor closes it.
	Client *cloudflare.Client

	// AccountID owns the namespace.
	AccountID string

	// NamespaceID is the KV namespace — the store the server keeps apart from
	// every other.
	NamespaceID string
}

// Client is a KV namespace you own.
type Client struct {
	inner     *cloudflare.Client
	account   string
	namespace string
}

// NewClient checks the namespace is reachable and returns a client over it.
//
// The check is a read of the namespace itself, so a token without access or an
// id that is wrong is learned here rather than in a request handler.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("workerskv: a cloudflare client is required")
	}
	if cfg.AccountID == "" || cfg.NamespaceID == "" {
		return nil, fmt.Errorf("workerskv: an account id and a namespace id are required")
	}
	_, err := cfg.Client.KV.Namespaces.Get(ctx, cfg.NamespaceID, kv.NamespaceGetParams{
		AccountID: cloudflare.F(cfg.AccountID),
	})
	if err != nil {
		return nil, fmt.Errorf("workerskv: cannot reach namespace %s: %w", cfg.NamespaceID, err)
	}
	return &Client{inner: cfg.Client, account: cfg.AccountID, namespace: cfg.NamespaceID}, nil
}

// Unwrap returns the SDK client, for anything this package does not do.
func (c *Client) Unwrap() *cloudflare.Client { return c.inner }

// Close releases nothing: the SDK client is an HTTP client with no shutdown of
// its own. It exists so a caller need not remember which backends need closing.
func (c *Client) Close() error { return nil }
