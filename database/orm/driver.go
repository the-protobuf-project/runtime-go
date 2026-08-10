package orm

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	"github.com/the-protobuf-project/runtime-go/database/core"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Driver is a store.Driver backed by a *gorm.DB. It drives every resource
// through GORM's dynamic map + Table API, so it needs no generated model types.
type Driver struct {
	db *gorm.DB

	// schema overrides the one each resource was generated with, and is empty
	// when a caller took the client's own. It is what makes a single set of
	// descriptors serve many tenants — see [Provider.SetDatabase].
	schema string
}

// New returns a Driver backed by db. Open db with gorm.Config{TranslateError:
// true} so error mapping (ErrAlreadyExists / ErrNotFound) works.
//
// This drives every resource on the schema its descriptor names. Use
// [NewProvider] to select one at runtime instead.
func New(db *gorm.DB) *Driver { return &Driver{db: db} }

// compile-time proof the GORM engine satisfies the backend-agnostic contract
// and the capabilities a relational backend has beyond it.
var (
	_ store.Driver        = (*Driver)(nil)
	_ store.Transactional = (*Driver)(nil)
	_ store.Migrator      = (*Driver)(nil)
)

// table returns the (optionally schema-qualified) table name GORM should target.
//
// Unquoted: GORM's Table() quotes what it is given for the dialect in play, and
// pre-quoting it produces a name quoted twice that matches nothing. Raw SQL —
// CREATE TABLE, DROP TABLE — needs the quoted form instead, which is
// [Driver.quotedTable].
//
// A schema selected at runtime wins over the one the descriptor was generated
// with: the descriptor says what a Book is, the selection says whose.
func (d *Driver) table(res *store.Resource) string {
	schema := d.schema
	if schema == "" {
		schema = res.Schema
	}
	if schema != "" {
		return schema + "." + res.Table
	}
	return res.Table
}

func (d *Driver) Create(ctx context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	cols, err := store.MessageToColumns(res, msg)
	if err != nil {
		return store.WriteResult{}, err
	}
	core.FillManaged(res, cols, true)
	tx := d.db.WithContext(ctx).Table(d.table(res)).Create(cols)
	if tx.Error != nil {
		if errors.Is(tx.Error, gorm.ErrDuplicatedKey) {
			return store.WriteResult{}, store.ErrAlreadyExists
		}
		return store.WriteResult{}, tx.Error
	}
	// What was stored, not what was handed in. FillManaged writes the generated
	// key and the audit timestamps into cols, and returning the caller's own
	// message would send none of them back — so a caller could not learn the id
	// of the record it had just created.
	out, err := store.ColumnsToMessage(res, cols)
	if err != nil {
		return store.WriteResult{}, err
	}
	return store.WriteResult{Message: out}, nil
}

func (d *Driver) Get(ctx context.Context, res *store.Resource, key string) (proto.Message, error) {
	rows, err := d.fetch(ctx, res, d.quote(res.PKColumn)+" = ?", []any{key}, "", 1, 0)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, store.ErrNotFound
	}
	return store.ColumnsToMessage(res, rows[0])
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
	// The PK is the lookup, not part of the SET clause — but it belongs in the
	// message that goes back, so it is removed after the result is built.
	updated := make(map[string]any, len(cols))
	for k, v := range cols {
		updated[k] = v
	}
	delete(updated, res.PKColumn)

	tx := d.db.WithContext(ctx).Table(d.table(res)).
		Where(map[string]any{res.PKColumn: key}).Updates(updated)
	if tx.Error != nil {
		return store.WriteResult{}, tx.Error
	}
	if tx.RowsAffected == 0 {
		return store.WriteResult{}, store.ErrNotFound
	}
	out, err := store.ColumnsToMessage(res, cols)
	if err != nil {
		return store.WriteResult{}, err
	}
	return store.WriteResult{Message: out}, nil
}

func (d *Driver) Delete(ctx context.Context, res *store.Resource, key string) error {
	tx := d.db.WithContext(ctx).Table(d.table(res)).
		Where(map[string]any{res.PKColumn: key}).Delete(nil)
	if tx.Error != nil {
		return tx.Error
	}
	if tx.RowsAffected == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (d *Driver) List(ctx context.Context, res *store.Resource, opts store.ListOptions) (store.ListResult, error) {
	where, args, err := d.buildWhere(res, opts.Filter)
	if err != nil {
		return store.ListResult{}, err
	}

	total := core.NoTotal
	if !opts.OmitTotal {
		var cerr error
		if total, cerr = d.Count(ctx, res, opts); cerr != nil {
			return store.ListResult{}, cerr
		}
	}
	limit := core.PageSize(opts.PageSize)
	offset := core.DecodeToken(opts.PageToken)
	rows, ferr := d.fetch(ctx, res, where, args, d.orderClause(res, opts.OrderBy), core.FetchLimit(limit, opts.OmitTotal), offset)
	if ferr != nil {
		return store.ListResult{}, ferr
	}
	// Trimmed before decoding, so the row read only to prove another page exists
	// is dropped as a map rather than after being turned into a message nobody
	// will see.
	rows, next := core.TrimPage(rows, offset, limit, total, opts.OmitTotal)

	items := make([]proto.Message, 0, len(rows))
	for _, row := range rows {
		m, merr := store.ColumnsToMessage(res, row)
		if merr != nil {
			return store.ListResult{}, merr
		}
		items = append(items, m)
	}
	return store.ListResult{Items: items, NextPageToken: next, Total: total}, nil
}

func (d *Driver) Count(ctx context.Context, res *store.Resource, opts store.ListOptions) (int64, error) {
	where, args, err := d.buildWhere(res, opts.Filter)
	if err != nil {
		return 0, err
	}
	q := d.db.WithContext(ctx).Table(d.table(res))
	if where != "" {
		q = q.Where(where, args...)
	}
	var n int64
	if cerr := q.Count(&n).Error; cerr != nil {
		return 0, cerr
	}
	return n, nil
}

func (d *Driver) Exists(ctx context.Context, res *store.Resource, key string) (bool, error) {
	var n int64
	err := d.db.WithContext(ctx).Table(d.table(res)).
		Where(map[string]any{res.PKColumn: key}).Count(&n).Error
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// fetch runs a SELECT into a slice of column maps, applying an optional WHERE,
// ORDER BY, LIMIT, and OFFSET. Used by Get (where+limit 1) and List.
func (d *Driver) fetch(ctx context.Context, res *store.Resource, where string, args []any, orderBy string, limit, offset int64) ([]map[string]any, error) {
	q := d.db.WithContext(ctx).Table(d.table(res))
	if where != "" {
		q = q.Where(where, args...)
	}
	if orderBy != "" {
		q = q.Order(orderBy)
	}
	if limit > 0 {
		q = q.Limit(int(limit))
	}
	if offset > 0 {
		q = q.Offset(int(offset))
	}
	var rows []map[string]any
	if err := q.Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// quotedTable is [Driver.table] ready to be interpolated into raw SQL, where
// nothing else will quote it.
func (d *Driver) quotedTable(res *store.Resource) string {
	schema := d.schema
	if schema == "" {
		schema = res.Schema
	}
	if schema != "" {
		return d.quote(schema) + "." + d.quote(res.Table)
	}
	return d.quote(res.Table)
}
