package d1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/cloudflare/cloudflare-go/v7"
	cfd1 "github.com/cloudflare/cloudflare-go/v7/d1"
	"github.com/cloudflare/cloudflare-go/v7/option"
)

// ErrUnique is what a statement that would duplicate a primary key or a unique
// column fails with. SQLite says UNIQUE for both, and D1 passes its message
// through, so the text is the only signal there is.
var ErrUnique = errors.New("d1: unique constraint failed")

// Config describes an account and the database it drives by default.
type Config struct {
	// AccountID owns the databases.
	AccountID string

	// APIToken needs D1 read and write. [NewClient] builds the SDK client from
	// it; [Wrap] ignores it, the client handed in carrying its own.
	APIToken string

	// DatabaseID is the database [Provider.SetDatabase] selects for an empty
	// name. It may be empty on a client that only ever selects by name.
	DatabaseID string
}

// Client is a D1 account you own: SQL in, rows out, and nothing about proto.
//
// It is the layer the records [Driver] is built on, usable on its own for the
// statements a resource descriptor cannot express — the D1 counterpart of
// mongodb.Client or a *gorm.DB.
type Client struct {
	sdk *cloudflare.Client
	cfg Config
}

// NewClient builds a client for the account in cfg, authenticated with its API
// token. Nothing is sent: a database is reached when one is queried or
// selected.
func NewClient(cfg Config) (*Client, error) {
	if cfg.APIToken == "" {
		return nil, fmt.Errorf("d1: an API token is required")
	}
	return newClient(cloudflare.NewClient(option.WithAPIToken(cfg.APIToken)), cfg)
}

// Wrap adopts an SDK client you built yourself, for options NewClient does not
// set: a base URL, retries, an HTTP client.
func Wrap(sdk *cloudflare.Client, cfg Config) (*Client, error) {
	if sdk == nil {
		return nil, fmt.Errorf("d1: a cloudflare client is required")
	}
	return newClient(sdk, cfg)
}

func newClient(sdk *cloudflare.Client, cfg Config) (*Client, error) {
	if cfg.AccountID == "" {
		return nil, fmt.Errorf("d1: an account id is required")
	}
	return &Client{sdk: sdk, cfg: cfg}, nil
}

// Unwrap returns the SDK client, for anything this package does not do.
func (c *Client) Unwrap() *cloudflare.Client { return c.sdk }

// Statement is one SQL statement and the values bound into it. The query
// endpoint binds every parameter as text; SQLite's column affinity turns a
// numeric string back into a number for an INTEGER or REAL column.
type Statement struct {
	SQL  string
	Args []string
}

// Result is what one statement produced.
type Result struct {
	// Rows are the rows a SELECT returned, column name to value. A number is
	// a [json.Number], so an integer past 2^53 keeps its digits.
	Rows []map[string]any

	// Changes is how many rows an INSERT, UPDATE or DELETE changed.
	Changes int64
}

// Query runs one statement against database databaseID.
func (c *Client) Query(ctx context.Context, databaseID string, s Statement) (Result, error) {
	body := cfd1.DatabaseQueryParamsBodyD1SingleQuery{Sql: cloudflare.F(s.SQL)}
	if len(s.Args) > 0 {
		body.Params = cloudflare.F(s.Args)
	}
	out, err := c.send(ctx, databaseID, body, 1)
	if err != nil {
		return Result{}, err
	}
	return out[0], nil
}

// Batch runs statements in one request, one result each, in order. D1 applies
// a batch as a single unit: a statement that fails rolls back the rest.
func (c *Client) Batch(ctx context.Context, databaseID string, stmts ...Statement) ([]Result, error) {
	items := make([]cfd1.DatabaseQueryParamsBodyMultipleQueriesBatch, len(stmts))
	for i, s := range stmts {
		items[i] = cfd1.DatabaseQueryParamsBodyMultipleQueriesBatch{Sql: cloudflare.F(s.SQL)}
		if len(s.Args) > 0 {
			items[i].Params = cloudflare.F(s.Args)
		}
	}
	return c.send(ctx, databaseID, cfd1.DatabaseQueryParamsBodyMultipleQueries{Batch: cloudflare.F(items)}, len(stmts))
}

// Ping reads database databaseID, proving it is there and the token reaches it.
func (c *Client) Ping(ctx context.Context, databaseID string) error {
	_, err := c.sdk.D1.Database.Get(ctx, databaseID, cfd1.DatabaseGetParams{AccountID: cloudflare.F(c.cfg.AccountID)})
	if err != nil {
		return fmt.Errorf("d1: database %s is not reachable: %w", databaseID, err)
	}
	return nil
}

// Lookup returns the id of the database named name.
//
// The list endpoint filters by name but matches loosely, so the result is
// checked for an exact match rather than trusted to be one: tenant_a must not
// select tenant_ab. A name that matches no database is an error rather than a
// database created on the spot: creating one is an account change.
func (c *Client) Lookup(ctx context.Context, name string) (string, error) {
	pages := c.sdk.D1.Database.ListAutoPaging(ctx, cfd1.DatabaseListParams{
		AccountID: cloudflare.F(c.cfg.AccountID),
		Name:      cloudflare.F(name),
	})
	for pages.Next() {
		if db := pages.Current(); db.Name == name {
			return db.UUID, nil
		}
	}
	if err := pages.Err(); err != nil {
		return "", fmt.Errorf("d1: cannot list databases: %w", err)
	}
	return "", fmt.Errorf("d1: no database named %q in the account; create it with `wrangler d1 create %s`", name, name)
}

// send posts a query body and decodes one result per statement.
func (c *Client) send(ctx context.Context, databaseID string, body cfd1.DatabaseQueryParamsBodyUnion, want int) ([]Result, error) {
	page, err := c.sdk.D1.Database.Query(ctx, databaseID, cfd1.DatabaseQueryParams{
		AccountID: cloudflare.F(c.cfg.AccountID),
		Body:      body,
	})
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return nil, fmt.Errorf("%w: %w", ErrUnique, err)
		}
		return nil, fmt.Errorf("d1: %w", err)
	}
	if len(page.Result) != want {
		return nil, fmt.Errorf("d1: sent %d statements and got %d results back", want, len(page.Result))
	}
	out := make([]Result, len(page.Result))
	for i := range page.Result {
		r := &page.Result[i]
		if !r.Success {
			return nil, fmt.Errorf("d1: statement %d did not succeed", i+1)
		}
		rows, derr := decodeRows(r.JSON.Results.Raw())
		if derr != nil {
			return nil, derr
		}
		out[i] = Result{Rows: rows, Changes: int64(r.Meta.Changes)}
	}
	return out, nil
}

// decodeRows reads the rows of one result, keeping every number as written.
//
// Decoding into float64 — what the SDK hands back — rounds an integer past 2^53
// before this package sees it. [json.Number] keeps the text.
func decodeRows(raw string) ([]map[string]any, error) {
	if raw == "" || raw == "null" {
		return nil, nil
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var rows []map[string]any
	if err := dec.Decode(&rows); err != nil {
		return nil, fmt.Errorf("d1: cannot read result rows: %w", err)
	}
	return rows, nil
}
