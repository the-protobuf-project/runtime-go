package orm

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/database/core"
)

// Driver is a database.Driver backed by a *gorm.DB. It drives every resource
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
	_ database.Driver        = (*Driver)(nil)
	_ database.Transactional = (*Driver)(nil)
	_ database.Migrator      = (*Driver)(nil)
)

// table returns the (optionally schema-qualified) table name GORM should target.
//
// A schema selected at runtime wins over the one the descriptor was generated
// with: the descriptor says what a Book is, the selection says whose.
func (d *Driver) table(res *database.Resource) string {
	schema := d.schema
	if schema == "" {
		schema = res.Schema
	}
	if schema != "" {
		return schema + "." + res.Table
	}
	return res.Table
}

func (d *Driver) Create(ctx context.Context, res *database.Resource, msg proto.Message) (database.WriteResult, error) {
	cols, err := database.MessageToColumns(res, msg)
	if err != nil {
		return database.WriteResult{}, err
	}
	core.FillManaged(res, cols, true)
	tx := d.db.WithContext(ctx).Table(d.table(res)).Create(cols)
	if tx.Error != nil {
		if errors.Is(tx.Error, gorm.ErrDuplicatedKey) {
			return database.WriteResult{}, database.ErrAlreadyExists
		}
		return database.WriteResult{}, tx.Error
	}
	return database.WriteResult{Message: msg}, nil
}

func (d *Driver) Get(ctx context.Context, res *database.Resource, key string) (proto.Message, error) {
	rows, err := d.fetch(ctx, res, map[string]any{res.PKColumn: key}, "", 1, 0)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, database.ErrNotFound
	}
	return database.ColumnsToMessage(res, rows[0])
}

func (d *Driver) Update(ctx context.Context, res *database.Resource, msg proto.Message) (database.WriteResult, error) {
	cols, err := database.MessageToColumns(res, msg)
	if err != nil {
		return database.WriteResult{}, err
	}
	key, err := database.KeyOf(res, msg)
	if err != nil {
		return database.WriteResult{}, err
	}
	core.FillManaged(res, cols, false)
	// The PK is the lookup, not part of the SET clause.
	delete(cols, res.PKColumn)
	tx := d.db.WithContext(ctx).Table(d.table(res)).
		Where(map[string]any{res.PKColumn: key}).Updates(cols)
	if tx.Error != nil {
		return database.WriteResult{}, tx.Error
	}
	if tx.RowsAffected == 0 {
		return database.WriteResult{}, database.ErrNotFound
	}
	return database.WriteResult{Message: msg}, nil
}

func (d *Driver) Delete(ctx context.Context, res *database.Resource, key string) error {
	tx := d.db.WithContext(ctx).Table(d.table(res)).
		Where(map[string]any{res.PKColumn: key}).Delete(nil)
	if tx.Error != nil {
		return tx.Error
	}
	if tx.RowsAffected == 0 {
		return database.ErrNotFound
	}
	return nil
}

func (d *Driver) List(ctx context.Context, res *database.Resource, opts database.ListOptions) (database.ListResult, error) {
	total, err := d.Count(ctx, res, opts)
	if err != nil {
		return database.ListResult{}, err
	}
	limit := core.PageSize(opts.PageSize)
	offset := core.DecodeToken(opts.PageToken)
	rows, err := d.fetch(ctx, res, nil, opts.OrderBy, limit, offset)
	if err != nil {
		return database.ListResult{}, err
	}
	items := make([]proto.Message, 0, len(rows))
	for _, row := range rows {
		m, err := database.ColumnsToMessage(res, row)
		if err != nil {
			return database.ListResult{}, err
		}
		items = append(items, m)
	}
	return database.ListResult{
		Items:         items,
		NextPageToken: core.EncodeToken(offset, int64(len(rows)), total),
		Total:         total,
	}, nil
}

func (d *Driver) Count(ctx context.Context, res *database.Resource, _ database.ListOptions) (int64, error) {
	var n int64
	if err := d.db.WithContext(ctx).Table(d.table(res)).Count(&n).Error; err != nil {
		return 0, err
	}
	return n, nil
}

func (d *Driver) Exists(ctx context.Context, res *database.Resource, key string) (bool, error) {
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
func (d *Driver) fetch(ctx context.Context, res *database.Resource, where map[string]any, orderBy string, limit, offset int64) ([]map[string]any, error) {
	q := d.db.WithContext(ctx).Table(d.table(res))
	if where != nil {
		q = q.Where(where)
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
