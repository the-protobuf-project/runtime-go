package d1

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database/core"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Driver is a store.Driver over one D1 database: proto records, read and written
// through their descriptors, on top of a [Client] that knows nothing of them.
type Driver struct {
	client     *Client
	databaseID string
}

// New returns a driver over database databaseID. Use [NewProvider] to select
// one by name at runtime instead.
func New(client *Client, databaseID string) *Driver {
	return &Driver{client: client, databaseID: databaseID}
}

// compile-time proof the D1 driver satisfies the contract and the capabilities
// it has beyond it.
var (
	_ store.Driver   = (*Driver)(nil)
	_ store.Migrator = (*Driver)(nil)
	_ store.Batcher  = (*Driver)(nil)
)

func (d *Driver) Create(ctx context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	s, cols, err := insert(res, msg)
	if err != nil {
		return store.WriteResult{}, err
	}
	if _, xerr := d.run(ctx, s); xerr != nil {
		return store.WriteResult{}, xerr
	}
	// What was stored, generated key and audit timestamps included, so a caller
	// learns the id of the record it just created.
	out, err := store.ColumnsToMessage(res, cols)
	if err != nil {
		return store.WriteResult{}, err
	}
	return store.WriteResult{Message: out}, nil
}

// insert builds the statement that stores msg, and the columns it stores.
//
// A nil column is left out rather than bound, because the endpoint binds text
// and NULL has none; an omitted column is NULL all the same.
func insert(res *store.Resource, msg proto.Message) (Statement, map[string]any, error) {
	cols, err := store.MessageToColumns(res, msg)
	if err != nil {
		return Statement{}, nil, err
	}
	core.FillManaged(res, cols, true)

	var names []string
	var args []string
	for _, c := range res.Columns {
		text, ok := bind(cols[c.Name])
		if !ok {
			continue
		}
		names = append(names, quote(c.Name))
		args = append(args, text)
	}
	if len(names) == 0 {
		return Statement{}, nil, fmt.Errorf("d1: %s has nothing to insert", res.Name)
	}
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quote(res.Table), strings.Join(names, ", "), placeholders(len(names)))
	return Statement{SQL: sql, Args: args}, cols, nil
}

func (d *Driver) Get(ctx context.Context, res *store.Resource, key string) (proto.Message, error) {
	out, err := d.run(ctx, Statement{
		SQL:  fmt.Sprintf("SELECT * FROM %s WHERE %s = ? LIMIT 1", quote(res.Table), quote(res.PKColumn)),
		Args: []string{key},
	})
	if err != nil {
		return nil, err
	}
	if len(out.Rows) == 0 {
		return nil, store.ErrNotFound
	}
	return message(res, out.Rows[0])
}

func (d *Driver) Update(ctx context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	cols, err := store.MessageToColumns(res, msg)
	if err != nil {
		return store.WriteResult{}, err
	}
	key, err := store.KeyOf(res, msg)
	if err != nil {
		return store.WriteResult{}, err
	}
	core.FillManaged(res, cols, false)

	var sets []string
	var args []string
	for _, c := range res.Columns {
		if c.Name == res.PKColumn {
			continue // the lookup, not part of the SET clause
		}
		v, present := cols[c.Name]
		if !present {
			continue
		}
		if text, ok := bind(v); ok {
			sets = append(sets, quote(c.Name)+" = ?")
			args = append(args, text)
		} else {
			sets = append(sets, quote(c.Name)+" = NULL")
		}
	}
	if len(sets) == 0 {
		// Nothing to change, but the record still has to be there.
		ok, eerr := d.Exists(ctx, res, key)
		if eerr != nil {
			return store.WriteResult{}, eerr
		}
		if !ok {
			return store.WriteResult{}, store.ErrNotFound
		}
	} else {
		out, xerr := d.run(ctx, Statement{
			SQL: fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?",
				quote(res.Table), strings.Join(sets, ", "), quote(res.PKColumn)),
			Args: append(args, key),
		})
		if xerr != nil {
			return store.WriteResult{}, xerr
		}
		if out.Changes == 0 {
			return store.WriteResult{}, store.ErrNotFound
		}
	}
	out, err := store.ColumnsToMessage(res, cols)
	if err != nil {
		return store.WriteResult{}, err
	}
	return store.WriteResult{Message: out}, nil
}

func (d *Driver) Delete(ctx context.Context, res *store.Resource, key string) error {
	out, err := d.run(ctx, Statement{
		SQL:  fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quote(res.Table), quote(res.PKColumn)),
		Args: []string{key},
	})
	if err != nil {
		return err
	}
	if out.Changes == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (d *Driver) List(ctx context.Context, res *store.Resource, opts store.ListOptions) (store.ListResult, error) {
	where, args, err := buildWhere(res, opts.Filter)
	if err != nil {
		return store.ListResult{}, err
	}

	total := core.NoTotal
	if !opts.OmitTotal {
		if total, err = d.count(ctx, res, where, args); err != nil {
			return store.ListResult{}, err
		}
	}
	limit := core.PageSize(opts.PageSize)
	offset := core.DecodeToken(opts.PageToken)

	sql := "SELECT * FROM " + quote(res.Table)
	if where != "" {
		sql += " WHERE " + where
	}
	sql += fmt.Sprintf(" ORDER BY %s LIMIT %d OFFSET %d",
		orderClause(res, opts.OrderBy), core.FetchLimit(limit, opts.OmitTotal), offset)

	out, err := d.run(ctx, Statement{SQL: sql, Args: args})
	if err != nil {
		return store.ListResult{}, err
	}
	// Trimmed before decoding, so the row read only to prove another page exists
	// is never turned into a message nobody will see.
	rows, next := core.TrimPage(out.Rows, offset, limit, total, opts.OmitTotal)

	items := make([]proto.Message, 0, len(rows))
	for _, row := range rows {
		m, merr := message(res, row)
		if merr != nil {
			return store.ListResult{}, merr
		}
		items = append(items, m)
	}
	return store.ListResult{Items: items, NextPageToken: next, Total: total}, nil
}

func (d *Driver) Count(ctx context.Context, res *store.Resource, opts store.ListOptions) (int64, error) {
	where, args, err := buildWhere(res, opts.Filter)
	if err != nil {
		return 0, err
	}
	return d.count(ctx, res, where, args)
}

func (d *Driver) count(ctx context.Context, res *store.Resource, where string, args []string) (int64, error) {
	sql := "SELECT COUNT(*) AS n FROM " + quote(res.Table)
	if where != "" {
		sql += " WHERE " + where
	}
	out, err := d.run(ctx, Statement{SQL: sql, Args: args})
	if err != nil {
		return 0, err
	}
	if len(out.Rows) == 0 {
		return 0, nil
	}
	n, err := fromWire(store.KindInt, out.Rows[0]["n"])
	if err != nil {
		return 0, fmt.Errorf("d1: cannot read a count: %w", err)
	}
	return n.(int64), nil
}

func (d *Driver) Exists(ctx context.Context, res *store.Resource, key string) (bool, error) {
	out, err := d.run(ctx, Statement{
		SQL:  fmt.Sprintf("SELECT 1 FROM %s WHERE %s = ? LIMIT 1", quote(res.Table), quote(res.PKColumn)),
		Args: []string{key},
	})
	if err != nil {
		return false, err
	}
	return len(out.Rows) > 0, nil
}

// message decodes one result row into the resource's message.
func message(res *store.Resource, row map[string]any) (proto.Message, error) {
	cols, err := typed(res, row)
	if err != nil {
		return nil, err
	}
	return store.ColumnsToMessage(res, cols)
}
