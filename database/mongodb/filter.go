package mongodb

import (
	"fmt"
	"strconv"
	"time"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// The filter grammar, kept apart from the codec that uses it.
//
// It covers conjunctions of comparisons and nothing else. That is a small
// fraction of AIP-160, and the smallness is the design: a backend that accepts
// the full grammar and honors a subset returns the wrong records with nothing to
// indicate it ignored anything, which is the failure mode this contract exists
// to avoid. Everything else is refused by name.

// mongoOperator maps a comparison to its Mongo query operator.
func mongoOperator(op string) string {
	switch op {
	case "!=":
		return "$ne"
	case ">":
		return "$gt"
	case ">=":
		return "$gte"
	case "<":
		return "$lt"
	case "<=":
		return "$lte"
	default:
		return "$eq"
	}
}

// coerce turns a filter's textual value into the type its column stores, so a
// comparison against a number is a number rather than a string that never
// matches.
func coerce(kind store.Kind, raw string) (any, error) {
	switch kind {
	case store.KindInt, store.KindEnum:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is not an integer", raw)
		}
		return n, nil
	case store.KindUint:
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is not an unsigned integer", raw)
		}
		if n > 1<<63-1 {
			return nil, fmt.Errorf("%q is too large to compare; values past 2^63 are stored as text", raw)
		}
		return int64(n), nil
	case store.KindFloat:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is not a number", raw)
		}
		return f, nil
	case store.KindBool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("%q is not a boolean", raw)
		}
		return b, nil
	case store.KindTimestamp:
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return nil, fmt.Errorf("%q is not an RFC 3339 timestamp", raw)
		}
		return t.UTC(), nil
	default:
		return raw, nil
	}
}
