package mongodb

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/protobuf/proto"

	"github.com/the-protobuf-project/runtime-go/database/core"
	"github.com/the-protobuf-project/runtime-go/database/store"
)

// toDocument turns a column map into the BSON document stored on the server.
//
// The primary key moves to _id and every other column keeps its own name, so a
// document written here is readable by anything that speaks MongoDB rather than
// only by this program.
func toDocument(res *store.Resource, cols map[string]any) (bson.M, error) {
	doc := make(bson.M, len(cols))
	for name, value := range cols {
		// An unset optional column is reported as nil by the bridge and stored
		// as null. The absence is the value, so it needs no conversion.
		v := any(nil)
		if value != nil {
			converted, err := toBSON(res, name, value)
			if err != nil {
				return nil, err
			}
			v = converted
		}
		if name == res.PKColumn {
			if v == nil {
				return nil, fmt.Errorf("mongodb: resource %q has no primary key value", res.Name)
			}
			doc["_id"] = v
			continue
		}
		doc[name] = v
	}
	return doc, nil
}

// fromDocument turns a stored document back into a column map for the bridge.
func fromDocument(res *store.Resource, doc bson.M) (proto.Message, error) {
	cols := make(map[string]any, len(doc))
	for name, value := range doc {
		if name == "_id" {
			cols[res.PKColumn] = fromBSON(value)
			continue
		}
		cols[name] = fromBSON(value)
	}
	return store.ColumnsToMessage(res, cols)
}

// toBSON narrows a bridge value to something BSON can carry.
//
// One case needs care. BSON has no unsigned 64-bit integer: the driver would
// encode a large uint64 as a negative int64 and hand it back as a different
// number, silently. Past what an int64 holds, the value is stored as a decimal
// string instead — [fromBSON] reads it back — so a counter that runs past
// 2^63 is slow to read rather than wrong.
// The value is never nil: [toDocument] handles that before calling here.
func toBSON(res *store.Resource, column string, value any) (any, error) {
	col, ok := res.LookupColumn(column)
	if !ok {
		return value, nil
	}
	switch col.Kind {
	case store.KindUint:
		u, ok := value.(uint64)
		if !ok {
			return value, nil
		}
		if u > math.MaxInt64 {
			// BSON has no unsigned 64-bit integer, and handing this to the
			// driver as-is stores it as a negative int64 that reads back as a
			// different number. Decimal128 holds it exactly and stays a number
			// on the server, so it still sorts and compares — which a wrapper
			// document pretending to be a number would not.
			d, derr := bson.ParseDecimal128(strconv.FormatUint(u, 10))
			if derr != nil {
				return nil, fmt.Errorf("mongodb: cannot store %d: %w", u, derr)
			}
			return d, nil
		}
		return int64(u), nil
	case store.KindTimestamp:
		t, ok := value.(time.Time)
		if !ok {
			return value, nil
		}
		// BSON stores milliseconds; keeping the value UTC means a round trip
		// does not depend on the reader's zone.
		return t.UTC(), nil
	default:
		return value, nil
	}
}

// fromBSON widens a stored value back to what the bridge expects.
//
// The bridge already converts between integer widths, so this only has to undo
// what toBSON did and unwrap the driver's own document types.
func fromBSON(value any) any {
	switch v := value.(type) {
	case bson.Decimal128:
		// Only a uint64 past MaxInt64 is stored this way; see toBSON.
		if u, err := strconv.ParseUint(v.String(), 10, 64); err == nil {
			return u
		}
		return v
	case bson.DateTime:
		return v.Time().UTC()
	case bson.Binary:
		return v.Data
	default:
		return value
	}
}

// parseFilter turns an AIP-160 expression into a Mongo filter.
//
// A deliberately small subset: conjunctions of `column op value`, where op is
// one of = != > >= < <=. It is small because the alternative is worse — a
// backend that accepts the whole grammar and quietly honors half of it returns
// the wrong records with no indication anything was ignored. An expression this
// cannot parse is refused, naming what it did not understand.
func parseFilter(res *store.Resource, expr string) (bson.M, error) {
	filter := bson.M{}
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return filter, nil
	}

	clauses, perr := core.ParseFilter(expr)
	if perr != nil {
		return nil, fmt.Errorf("mongodb: %w", perr)
	}
	for _, c := range clauses {
		column, op, value := c.Column, c.Op, c.Value
		col, ok := res.LookupColumn(column)
		if !ok {
			return nil, fmt.Errorf("mongodb: filter names %q, which resource %q has no column for", column, res.Name)
		}
		field := column
		if col.PrimaryKey {
			field = "_id"
		}
		typed, err := coerce(col.Kind, value)
		if err != nil {
			return nil, fmt.Errorf("mongodb: filter on %q: %w", column, err)
		}
		if op == "=" {
			filter[field] = typed
			continue
		}
		filter[field] = bson.M{mongoOperator(op): typed}
	}
	return filter, nil
}

// mongoFindOneIDOnly projects away everything but _id, so an existence check
// does not transfer a document to throw it away.
func mongoFindOneIDOnly() options.Lister[options.FindOneOptions] {
	return options.FindOne().SetProjection(bson.M{"_id": 1})
}

// mongoFindPage builds the sort, limit and skip for one page.
func mongoFindPage(sort bson.D, limit, offset int64) options.Lister[options.FindOptions] {
	o := options.Find().SetSort(sort).SetLimit(limit)
	if offset > 0 {
		o = o.SetSkip(offset)
	}
	return o
}
