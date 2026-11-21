package d1

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cloudflare/cloudflare-go/v7"
	cfd1 "github.com/cloudflare/cloudflare-go/v7/d1"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// maxParams is how many values D1 binds into one statement. A bulk read is cut
// into pieces under it rather than refused at the server.
const maxParams = 100

// stampLayout writes a timestamp at a fixed width — every fraction digit, and
// always Z — so two of them compare as text exactly as they compare as times.
// RFC3339Nano drops trailing zeros, and "…:05Z" sorts after "…:05.5Z".
const stampLayout = "2006-01-02T15:04:05.000000000Z"

// statement is one SQL statement and the values bound into it.
type statement struct {
	sql  string
	args []string
}

// outcome is what one statement produced.
type outcome struct {
	rows    []map[string]any
	changes int64
}

// exec runs one statement.
func (d *Driver) exec(ctx context.Context, s statement) (outcome, error) {
	body := cfd1.DatabaseQueryParamsBodyD1SingleQuery{Sql: cloudflare.F(s.sql)}
	if len(s.args) > 0 {
		body.Params = cloudflare.F(s.args)
	}
	out, err := d.send(ctx, body, 1)
	if err != nil {
		return outcome{}, err
	}
	return out[0], nil
}

// batch runs statements in one request, which D1 applies as a single unit.
func (d *Driver) batch(ctx context.Context, stmts []statement) ([]outcome, error) {
	items := make([]cfd1.DatabaseQueryParamsBodyMultipleQueriesBatch, len(stmts))
	for i, s := range stmts {
		items[i] = cfd1.DatabaseQueryParamsBodyMultipleQueriesBatch{Sql: cloudflare.F(s.sql)}
		if len(s.args) > 0 {
			items[i].Params = cloudflare.F(s.args)
		}
	}
	return d.send(ctx, cfd1.DatabaseQueryParamsBodyMultipleQueries{Batch: cloudflare.F(items)}, len(stmts))
}

// send posts a query body and decodes one outcome per statement.
func (d *Driver) send(ctx context.Context, body cfd1.DatabaseQueryParamsBodyUnion, want int) ([]outcome, error) {
	page, err := d.client.D1.Database.Query(ctx, d.databaseID, cfd1.DatabaseQueryParams{
		AccountID: cloudflare.F(d.accountID),
		Body:      body,
	})
	if err != nil {
		return nil, classify(err)
	}
	if len(page.Result) != want {
		return nil, fmt.Errorf("d1: sent %d statements and got %d results back", want, len(page.Result))
	}
	out := make([]outcome, len(page.Result))
	for i := range page.Result {
		r := &page.Result[i]
		if !r.Success {
			return nil, fmt.Errorf("d1: statement %d did not succeed", i+1)
		}
		rows, derr := decodeRows(r.JSON.Results.Raw())
		if derr != nil {
			return nil, derr
		}
		out[i] = outcome{rows: rows, changes: int64(r.Meta.Changes)}
	}
	return out, nil
}

// decodeRows reads the rows of one result, keeping every number as written.
//
// Decoding into float64 — what the SDK hands back — rounds an integer past 2^53
// before this package sees it. [json.Number] keeps the text, and the column's
// kind decides what it becomes; see [typed].
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

// classify maps a D1 failure to the sentinel a caller tests for.
//
// SQLite says UNIQUE for both a duplicate primary key and a duplicate unique
// column, and D1 passes the message through, so the text is the only signal
// there is.
func classify(err error) error {
	if strings.Contains(err.Error(), "UNIQUE constraint failed") {
		return fmt.Errorf("%w: %w", store.ErrAlreadyExists, err)
	}
	return fmt.Errorf("d1: %w", err)
}

// bind renders a value as the text D1 binds. The second result is false for a
// nil, which has no text and has to be written as a NULL literal instead.
func bind(v any) (string, bool) {
	switch x := v.(type) {
	case nil:
		return "", false
	case string:
		return x, true
	case []byte:
		return base64.StdEncoding.EncodeToString(x), true
	case bool:
		if x {
			return "1", true
		}
		return "0", true
	case int:
		return strconv.Itoa(x), true
	case int32:
		return strconv.FormatInt(int64(x), 10), true
	case int64:
		return strconv.FormatInt(x, 10), true
	case uint32:
		return strconv.FormatUint(uint64(x), 10), true
	case uint64:
		return strconv.FormatUint(x, 10), true
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32), true
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), true
	case time.Time:
		return x.UTC().Format(stampLayout), true
	default:
		return fmt.Sprint(x), true
	}
}

// typed turns a decoded result row into the Go values [store.ColumnsToMessage]
// reads, column by column through the descriptor.
func typed(res *store.Resource, row map[string]any) (map[string]any, error) {
	for _, c := range res.Columns {
		v, ok := row[c.Name]
		if !ok || v == nil {
			continue
		}
		out, err := fromWire(c.Kind, v)
		if err != nil {
			return nil, fmt.Errorf("d1: %s.%s: %w", res.Table, c.Name, err)
		}
		row[c.Name] = out
	}
	return row, nil
}

// fromWire reads one JSON value as the kind its column holds.
func fromWire(kind store.Kind, v any) (any, error) {
	text := func() string {
		if n, ok := v.(json.Number); ok {
			return n.String()
		}
		return fmt.Sprint(v)
	}
	switch kind {
	case store.KindInt, store.KindEnum:
		return strconv.ParseInt(text(), 10, 64)
	case store.KindUint:
		return strconv.ParseUint(text(), 10, 64)
	case store.KindFloat:
		return strconv.ParseFloat(text(), 64)
	case store.KindBool:
		switch b := v.(type) {
		case bool:
			return b, nil
		default:
			n, err := strconv.ParseFloat(text(), 64)
			return n != 0, err
		}
	case store.KindBytes:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("bytes stored as %T, not base64 text", v)
		}
		return base64.StdEncoding.DecodeString(s)
	case store.KindTimestamp:
		return text(), nil
	default:
		return text(), nil
	}
}

// quote wraps an identifier. Column and table names come from a descriptor
// rather than a request, so this is defense in depth — but a descriptor is
// data, and data reaching an identifier position gets quoted.
func quote(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// placeholders is n question marks, comma-separated.
func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}
