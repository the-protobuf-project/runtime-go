package redis

import (
	"context"
	"fmt"

	"github.com/the-protobuf-project/runtime-go/redis/internal/conn"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Config wires a [Client].
type Config struct {
	// Address is the server host, without a port. Defaults to localhost.
	Address string

	// Port is the server port. Defaults to 6379.
	Port string

	// Username and Password authenticate the connection, when the server
	// requires it.
	Username string
	Password string

	// Prefix namespaces every key this client's operations read and write.
	//
	// Use it to share one Redis database between concerns, or to run
	// independent instances of the same concern side by side. Named databases
	// give stronger isolation; a prefix is the lighter option.
	Prefix string

	// Logger receives the provider's own records — which key an id resolved to,
	// which stale entries were swept, which database a manager bound to.
	// Defaults to [telemetry.NoopLogger].
	//
	// This is the provider's internal detail. For a uniform record per
	// operation, wrap the returned handlers with the concern's WithLogging
	// decorator; the two compose.
	Logger telemetry.Logger

	// Meter receives the provider's own measurements. Defaults to
	// [telemetry.NoopMeter].
	Meter telemetry.Meter
}

// Client is a connection to a Redis server and the entry point to its named
// databases.
//
// It owns the connection it opened and releases it in [Client.Close]. Managers
// made by [Client.SetDatabase] own connections of their own — closing this
// client does not close theirs.
type Client struct {
	conn   *conn.Conn
	prefix string
	log    telemetry.Logger
	meter  telemetry.Meter
}

// New opens a client and verifies the server answers.
//
// It dials here rather than lazily so an unreachable server fails where the
// caller can still report it, instead of on the first operation.
func New(ctx context.Context, cfg Config) (*Client, error) {
	log := cfg.Logger
	if log == nil {
		log = telemetry.NoopLogger
	}
	meter := cfg.Meter
	if meter == nil {
		meter = telemetry.NoopMeter
	}

	c, err := conn.Open(ctx, conn.Options{
		Address:  cfg.Address,
		Port:     cfg.Port,
		Username: cfg.Username,
		Password: cfg.Password,
	}, log)
	if err != nil {
		return nil, err
	}

	return &Client{conn: c, prefix: cfg.Prefix, log: log, meter: meter}, nil
}

// CreateDatabase registers a name and assigns it the next free database index.
//
// It reports an error when the name already exists — the caller asked to create
// something and did not. Use [Client.GetDatabase] to test for one first, or
// ignore the error when re-running setup is expected.
func (c *Client) CreateDatabase(ctx context.Context, name string) error {
	_, err := c.conn.CreateDatabase(ctx, name)
	return err
}

// GetDatabase returns the index a name maps to.
func (c *Client) GetDatabase(ctx context.Context, name string) (int, error) {
	return c.conn.LookupDatabase(ctx, name)
}

// ListDatabases returns every registered name and its index.
func (c *Client) ListDatabases(ctx context.Context) (map[string]int, error) {
	return c.conn.ListDatabases(ctx)
}

// DeleteDatabase drops a name and flushes everything stored under it.
func (c *Client) DeleteDatabase(ctx context.Context, name string) error {
	return c.conn.DeleteDatabase(ctx, name)
}

// SetDatabase opens the named database and returns a manager over it.
//
// The manager holds its own connection, so this client is unchanged and any
// manager made earlier keeps working. Close the manager when you are done with
// it; closing this client does not.
func (c *Client) SetDatabase(ctx context.Context, name string) (*DBManager, error) {
	index, err := c.conn.LookupDatabase(ctx, name)
	if err != nil {
		return nil, err
	}

	bound, err := c.conn.Bind(ctx, index)
	if err != nil {
		return nil, fmt.Errorf("redis: cannot open database %q: %w", name, err)
	}

	log := c.log.With(telemetry.Fields{"database": name, "db_index": index})
	log.Debug(ctx, "database selected", nil)

	return newDBManager(bound, c.prefix, name, log, c.meter), nil
}

// Ping verifies the server is still reachable.
func (c *Client) Ping(ctx context.Context) error {
	if err := c.conn.Redis().Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: ping failed: %w", err)
	}
	return nil
}

// Close releases this client's connection. Managers made by
// [Client.SetDatabase] hold their own and are closed separately.
func (c *Client) Close() error {
	return c.conn.Close()
}
