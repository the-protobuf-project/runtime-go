package database

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// Coll is a typed view over one Go struct, and the ordinary way to use this
// module.
//
//	type Book struct {
//	    ID    string `db:"id,pk"`
//	    Title string `db:"title,unique"`
//	    Year  int32  `db:"published_year"`
//	}
//
//	books, _ := database.Collection[Book](db, "books")
//	books.EnsureSchema(ctx)
//
//	id, _ := books.Create(ctx, Book{ID: "books/dune", Title: "Dune", Year: 1965})
//	b, _ := books.Get(ctx, id)     // already a Book
//
// It is a view, not a client: the [store.DB] underneath is shared, so one
// connection serves every type in a program and a Coll costs nothing to make.
//
// Everything reachable here is also reachable through [store] directly, on
// proto messages and generated descriptors. This is the same layer with the
// protobuf hidden — a struct stored here and a message stored through store are
// the same rows.
type Coll[T any] struct {
	db  *store.DB
	res *store.Resource
}

// Collection returns a view of db bound to T, stored in the named table.
//
// An empty table takes the struct's name pluralized, which is the convention
// most callers would have written anyway.
func Collection[T any](db *store.DB, table string) (*Coll[T], error) {
	if db == nil {
		return nil, fmt.Errorf("database: Collection needs a database")
	}
	res, err := Describe[T](table)
	if err != nil {
		return nil, err
	}
	return &Coll[T]{db: db, res: res}, nil
}

// MustCollection is [Collection] for a binding settled at build time, panicking
// rather than returning an error.
//
// A struct that cannot be described is a mistake in the struct — a missing pk,
// a field of a type no backend has — and it is the same mistake on every run.
// Use it for a package-level variable; use [Collection] where the table comes
// from configuration.
func MustCollection[T any](db *store.DB, table string) *Coll[T] {
	c, err := Collection[T](db, table)
	if err != nil {
		panic(err)
	}
	return c
}

// Resource returns the descriptor this view derived, for the operations it does
// not cover — a graph edge, a time-series window — which take one directly.
func (c *Coll[T]) Resource() *store.Resource { return c.res }

// DB returns the database underneath, on the same terms.
func (c *Coll[T]) DB() *store.DB { return c.db }

// EnsureSchema creates what the struct describes, and is safe to call
// repeatedly.
func (c *Coll[T]) EnsureSchema(ctx context.Context) error {
	return c.db.Schema.EnsureSchema(ctx, c.res)
}

// DropSchema removes it, and everything in it.
func (c *Coll[T]) DropSchema(ctx context.Context) error {
	return c.db.Schema.DropSchema(ctx, c.res)
}

// Create stores value and returns the key it was stored under.
//
// A key the driver generated — from a field tagged ulid or uuid — comes back
// here, which is the only place a caller can learn it.
func (c *Coll[T]) Create(ctx context.Context, value T) (string, error) {
	msg, err := c.toMessage(value)
	if err != nil {
		return "", err
	}
	out, err := c.db.Create(ctx, c.res, msg)
	if err != nil {
		return "", err
	}
	return store.KeyOf(c.res, out.Message)
}

// Get returns the record under key, decoded as T.
func (c *Coll[T]) Get(ctx context.Context, key string) (T, error) {
	var zero T
	msg, err := c.db.Get(ctx, c.res, key)
	if err != nil {
		return zero, err
	}
	return c.fromMessage(msg)
}

// Update replaces the record identified by value's key.
func (c *Coll[T]) Update(ctx context.Context, value T) error {
	msg, err := c.toMessage(value)
	if err != nil {
		return err
	}
	_, err = c.db.Update(ctx, c.res, msg)
	return err
}

// Delete removes the record under key.
func (c *Coll[T]) Delete(ctx context.Context, key string) error {
	return c.db.Delete(ctx, c.res, key)
}

// Exists reports whether a record with the given key is there.
func (c *Coll[T]) Exists(ctx context.Context, key string) (bool, error) {
	return c.db.Exists(ctx, c.res, key)
}

// Count returns how many records match opts.Filter.
func (c *Coll[T]) Count(ctx context.Context, opts ...ListOption) (int64, error) {
	return c.db.Count(ctx, c.res, listOptions(opts))
}

// List returns a page of records as T, with the token for the next page.
//
// The total is deliberately not returned here: computing it doubles the cost of
// a listing, and a caller that needs it asks [Coll.Count]. See
// [store.ListOptions.OmitTotal].
func (c *Coll[T]) List(ctx context.Context, opts ...ListOption) ([]T, string, error) {
	o := listOptions(opts)
	o.OmitTotal = true

	out, err := c.db.List(ctx, c.res, o)
	if err != nil {
		return nil, "", err
	}
	items := make([]T, 0, len(out.Items))
	for _, msg := range out.Items {
		v, cerr := c.fromMessage(msg)
		if cerr != nil {
			return nil, "", cerr
		}
		items = append(items, v)
	}
	return items, out.NextPageToken, nil
}

// All pages through every record matching opts.
//
// It exists because the paging loop is the same four lines every time and one of
// them is easy to get wrong. It is still a loop over pages: a table with a
// million rows costs a million rows of memory, so use [Coll.List] where that
// matters.
func (c *Coll[T]) All(ctx context.Context, opts ...ListOption) ([]T, error) {
	// A copy with room for the page token, because appending to the caller's
	// variadic slice would write into memory it still owns whenever that slice
	// had spare capacity.
	o := make([]ListOption, len(opts), len(opts)+1)
	copy(o, opts)

	var (
		out  []T
		seen = map[string]bool{}
	)
	token := ""
	for {
		page, next, err := c.List(ctx, append(o, pageToken(token))...)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if next == "" {
			return out, nil
		}
		if seen[next] {
			return nil, fmt.Errorf("database: %s repeated page token %q; refusing to loop", c.res.Name, next)
		}
		seen[next] = true
		token = next
	}
}

// toMessage fills the proto message the layer underneath moves values through.
//
// Struct to columns to message, using the bridge every driver already uses, so
// a value written here is indistinguishable from one written through [store].
func (c *Coll[T]) toMessage(value T) (proto.Message, error) {
	cols, err := c.toColumns(value)
	if err != nil {
		return nil, err
	}
	return store.ColumnsToMessage(c.res, cols)
}

func (c *Coll[T]) fromMessage(msg proto.Message) (T, error) {
	var out T
	cols, err := store.MessageToColumns(c.res, msg)
	if err != nil {
		return out, err
	}
	if cerr := c.fromColumns(cols, &out); cerr != nil {
		return out, cerr
	}
	return out, nil
}

// toColumns reads the struct's fields into a column map.
func (c *Coll[T]) toColumns(value T) (map[string]any, error) {
	fields, ok := fieldsByResource.Load(c.res)
	if !ok {
		return nil, fmt.Errorf("database: %s was not described by this package", c.res.Name)
	}
	rv := reflect.ValueOf(value)
	out := make(map[string]any, len(fields.([]field)))
	for _, f := range fields.([]field) {
		fv := rv.Field(f.index)
		out[f.column.Name] = normalize(fv, f.column.Kind)
	}
	return out, nil
}

// fromColumns writes a column map back into a struct.
func (c *Coll[T]) fromColumns(cols map[string]any, dest *T) error {
	fields, ok := fieldsByResource.Load(c.res)
	if !ok {
		return fmt.Errorf("database: %s was not described by this package", c.res.Name)
	}
	rv := reflect.ValueOf(dest).Elem()
	for _, f := range fields.([]field) {
		raw, present := cols[f.column.Name]
		if !present || raw == nil {
			continue // unset stays the zero value
		}
		if err := assign(rv.Field(f.index), raw); err != nil {
			return fmt.Errorf("database: %s.%s: %w", c.res.Name, f.column.Name, err)
		}
	}
	return nil
}

// normalize widens a struct field to the Go type the bridge expects for its
// kind, so the conversion below it has one shape per kind rather than one per
// Go type.
func normalize(v reflect.Value, kind store.Kind) any {
	switch kind {
	case store.KindInt:
		return v.Int()
	case store.KindUint:
		return v.Uint()
	case store.KindFloat:
		return v.Float()
	case store.KindBool:
		return v.Bool()
	case store.KindString:
		return v.String()
	case store.KindBytes:
		return v.Bytes()
	case store.KindTimestamp:
		if t, ok := v.Interface().(time.Time); ok {
			return t.UTC()
		}
	}
	return v.Interface()
}

// assign narrows a stored value back into a struct field.
//
// Conversion is restricted to kinds that mean the same thing on both sides. Go's
// own conversion rules are wider than that in a way that would corrupt data
// silently: an int64 is convertible to a string, so a backend handing back 65
// for a string column would produce "A" rather than "65", and the error path
// that exists to catch exactly that would never run.
func assign(dst reflect.Value, raw any) error {
	rv := reflect.ValueOf(raw)
	if rv.Type().AssignableTo(dst.Type()) {
		dst.Set(rv)
		return nil
	}
	if compatible(rv.Kind(), dst.Kind()) && rv.Type().ConvertibleTo(dst.Type()) {
		if lossy(rv, dst.Type()) {
			return fmt.Errorf("%v does not fit in a %s", rv.Interface(), dst.Type())
		}
		dst.Set(rv.Convert(dst.Type()))
		return nil
	}
	return fmt.Errorf("cannot put a %T into a %s", raw, dst.Type())
}

// compatible reports whether two kinds are the same sort of thing, so a
// conversion between them preserves the value rather than reinterpreting it.
func compatible(from, to reflect.Kind) bool {
	group := func(k reflect.Kind) int {
		switch k {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return 1
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return 1
		case reflect.Float32, reflect.Float64:
			return 2
		case reflect.String:
			return 3
		case reflect.Bool:
			return 4
		case reflect.Slice:
			return 5
		default:
			return 0
		}
	}
	// Integers and floats convert between each other; everything else must stay
	// in its own group.
	f, t := group(from), group(to)
	if f == 0 || t == 0 {
		return false
	}
	if f <= 2 && t <= 2 {
		return true
	}
	return f == t
}

// lossy reports whether a numeric conversion would not round-trip.
func lossy(v reflect.Value, to reflect.Type) bool {
	out := v.Convert(to)
	switch out.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		back := out.Convert(v.Type())
		return !back.Equal(v)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		back := out.Convert(v.Type())
		return !back.Equal(v)
	default:
		return false
	}
}
