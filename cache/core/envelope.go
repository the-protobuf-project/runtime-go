package core

import (
	"encoding/json"
	"fmt"
	"time"
)

// envelope frames a read-through value with the moment it stops being fresh.
//
// Two deadlines are needed and a backend only stores one. The expiry it holds is
// the hard one — past it the entry is gone — while freshness has to travel with
// the value, because "still servable, but somebody should go and refresh it" is
// not a state any cache protocol has.
//
// Only [aside] writes these. Document and Volatile store values as they are, so
// nothing else in the cache has to know this framing exists, and an entry
// written by one of them is readable by anything.
type envelope struct {
	// Value is the caller's value, unexamined.
	Value json.RawMessage `json:"v"`

	// Fresh is when it stops being fresh, in Unix milliseconds. Zero means it is
	// fresh for as long as it exists, which is the behavior when no stale window
	// was asked for.
	Fresh int64 `json:"f,omitempty"`
}

// pack wraps a value, marking it fresh for the given window.
func pack(value any, fresh time.Duration) ([]byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("cache: cannot encode %T: %w", value, err)
	}
	e := envelope{Value: body}
	if fresh > 0 {
		e.Fresh = time.Now().Add(fresh).UnixMilli()
	}
	out, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("cache: cannot frame %T: %w", value, err)
	}
	return out, nil
}

// unpack returns the value and whether it is past its freshness deadline.
func unpack(body []byte) (json.RawMessage, bool, error) {
	var e envelope
	if err := json.Unmarshal(body, &e); err != nil {
		return nil, false, fmt.Errorf("cache: cannot read the stored frame: %w", err)
	}
	if e.Fresh == 0 {
		return e.Value, false, nil
	}
	return e.Value, time.Now().UnixMilli() > e.Fresh, nil
}
