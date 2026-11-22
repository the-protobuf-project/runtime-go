package d1

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// maxParams is how many values D1 binds into one statement. A bulk read is cut
// into pieces under it rather than refused at the server.
const maxParams = 100

// stampLayout writes a timestamp at a fixed width — every fraction digit, and
// always Z — so two of them compare as text exactly as they compare as times.
// RFC3339Nano drops trailing zeros, and "…:05Z" sorts after "…:05.5Z".
const stampLayout = "2006-01-02T15:04:05.000000000Z"

// run sends one statement to the driver's database, reporting a failure in the
// records contract's terms.
func (d *Driver) run(ctx context.Context, s Statement) (Result, error) {
	r, err := d.client.Query(ctx, d.databaseID, s)
	return r, contract(err)
}

// runBatch sends statements in one request, which D1 applies as a single unit.
func (d *Driver) runBatch(ctx context.Context, stmts []Statement) ([]Result, error) {
	r, err := d.client.Batch(ctx, d.databaseID, stmts...)
	return r, contract(err)
}

// contract maps the client's [ErrUnique] to [store.ErrAlreadyExists], the
// sentinel a records caller tests for, and leaves every other failure alone.
func contract(err error) error {
	if errors.Is(err, ErrUnique) {
		return fmt.Errorf("%w: %w", store.ErrAlreadyExists, err)
	}
	return err
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
