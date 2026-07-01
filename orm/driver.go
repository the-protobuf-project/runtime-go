package orm

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	"github.com/the-protobuf-project/runtime-go/store"
)

// defaultPageSize bounds a List call that does not specify opts.PageSize.
const defaultPageSize = 50

// Driver is a store.Driver backed by a *gorm.DB. It drives every resource
// through GORM's dynamic map + Table API, so it needs no generated model types.
type Driver struct{ db *gorm.DB }

// New returns a Driver backed by db. Open db with gorm.Config{TranslateError:
// true} so error mapping (ErrAlreadyExists / ErrNotFound) works.
func New(db *gorm.DB) *Driver { return &Driver{db: db} }

// compile-time proof the GORM engine satisfies the backend-agnostic contract.
var _ store.Driver = (*Driver)(nil)

// table returns the (optionally schema-qualified) table name GORM should target.
func table(res *store.Resource) string {
	if res.Schema != "" {
		return res.Schema + "." + res.Table
	}
	return res.Table
}

func (d *Driver) Create(ctx context.Context, res *store.Resource, msg proto.Message) (store.WriteResult, error) {
	cols, err := store.MessageToColumns(res, msg)
	if err != nil {
		return store.WriteResult{}, err
	}
	fillManaged(res, cols, true)
	tx := d.db.WithContext(ctx).Table(table(res)).Create(cols)
	if tx.Error != nil {
		if errors.Is(tx.Error, gorm.ErrDuplicatedKey) {
			return store.WriteResult{}, store.ErrAlreadyExists
		}
		return store.WriteResult{}, tx.Error
	}
	return store.WriteResult{Message: msg}, nil
}

func (d *Driver) Get(ctx context.Context, res *store.Resource, key string) (proto.Message, error) {
	rows, err := d.fetch(ctx, res, map[string]any{res.PKColumn: key}, "", 1, 0)
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
	fillManaged(res, cols, false)
	// The PK is the lookup, not part of the SET clause.
	delete(cols, res.PKColumn)
	tx := d.db.WithContext(ctx).Table(table(res)).
		Where(map[string]any{res.PKColumn: key}).Updates(cols)
	if tx.Error != nil {
		return store.WriteResult{}, tx.Error
	}
	if tx.RowsAffected == 0 {
		return store.WriteResult{}, store.ErrNotFound
	}
	return store.WriteResult{Message: msg}, nil
}

func (d *Driver) Delete(ctx context.Context, res *store.Resource, key string) error {
	tx := d.db.WithContext(ctx).Table(table(res)).
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
	total, err := d.Count(ctx, res, opts)
	if err != nil {
		return store.ListResult{}, err
	}
	limit := int(opts.PageSize)
	if limit <= 0 {
		limit = defaultPageSize
	}
	offset := decodeToken(opts.PageToken)
	rows, err := d.fetch(ctx, res, nil, opts.OrderBy, limit, offset)
	if err != nil {
		return store.ListResult{}, err
	}
	items := make([]proto.Message, 0, len(rows))
	for _, row := range rows {
		m, err := store.ColumnsToMessage(res, row)
		if err != nil {
			return store.ListResult{}, err
		}
		items = append(items, m)
	}
	// A full page implies there may be more; hand back an offset-based token.
	next := ""
	if len(rows) == limit && int64(offset+limit) < total {
		next = strconv.Itoa(offset + limit)
	}
	return store.ListResult{Items: items, NextPageToken: next, Total: total}, nil
}

func (d *Driver) Count(ctx context.Context, res *store.Resource, _ store.ListOptions) (int64, error) {
	var n int64
	if err := d.db.WithContext(ctx).Table(table(res)).Count(&n).Error; err != nil {
		return 0, err
	}
	return n, nil
}

func (d *Driver) Exists(ctx context.Context, res *store.Resource, key string) (bool, error) {
	var n int64
	err := d.db.WithContext(ctx).Table(table(res)).
		Where(map[string]any{res.PKColumn: key}).Count(&n).Error
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// fetch runs a SELECT into a slice of column maps, applying an optional WHERE,
// ORDER BY, LIMIT, and OFFSET. Used by Get (where+limit 1) and List.
func (d *Driver) fetch(ctx context.Context, res *store.Resource, where map[string]any, orderBy string, limit, offset int) ([]map[string]any, error) {
	q := d.db.WithContext(ctx).Table(table(res))
	if where != nil {
		q = q.Where(where)
	}
	if orderBy != "" {
		q = q.Order(orderBy)
	}
	if limit > 0 {
		q = q.Limit(limit)
	}
	if offset > 0 {
		q = q.Offset(offset)
	}
	var rows []map[string]any
	if err := q.Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// fillManaged supplies the values for driver-managed columns the proto message
// does not carry: a generated primary key (ULID/UUID) and audit timestamps.
// onCreate distinguishes Create (generate keys, set both create and update
// timestamps) from Update (touch only AutoUpdate columns).
func fillManaged(res *store.Resource, cols map[string]any, onCreate bool) {
	now := time.Now().UTC()
	for _, c := range res.Columns {
		switch {
		case onCreate && c.Generated != "" && isEmpty(cols[c.Name]):
			cols[c.Name] = generateID(c.Generated)
		case onCreate && (c.AutoCreate || c.AutoUpdate):
			cols[c.Name] = now
		case !onCreate && c.AutoUpdate:
			cols[c.Name] = now
		}
	}
}

// generateID returns a new identifier for the named strategy.
func generateID(strategy string) string {
	switch strategy {
	case "uuid":
		return uuid.NewString()
	default: // "ulid"
		return ulid.MustNew(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano()))).String()
	}
}

func isEmpty(v any) bool {
	if v == nil {
		return true
	}
	s, ok := v.(string)
	return ok && s == ""
}

// decodeToken parses an offset-based page token, treating an empty or malformed
// token as offset 0.
func decodeToken(token string) int {
	if token == "" {
		return 0
	}
	n, err := strconv.Atoi(token)
	if err != nil || n < 0 {
		return 0
	}
	return n
}
