package core

import (
	"encoding/json"
	"fmt"
	"time"
)

// envelope frames a read-through value with the moment it stops being fresh,
// and carries a remembered absence in the same slot.
//
// Two deadlines are needed and a backend only stores one. The expiry it holds is
// the hard one — past it the entry is gone — while freshness has to travel with
// the value, because "still servable, but somebody should go and refresh it" is
// not a state any cache protocol has.
//
// The absence rides here rather than under a key of its own, and that is worth
// spelling out because it used to be separate. A miss then cost two round trips:
// one to find no value, another to find no tombstone — and both ran on every
// caller, before any of them reached the single-flight. A thousand callers
// arriving on one cold key made two thousand requests to answer one question.
// In one slot, a miss is one GET and the answer comes back with it.
//
// Only [aside] writes these. Document and Volatile store values as they are, so
// nothing else in the cache has to know this framing exists, and an entry
// written by one of them is readable by anything.
type envelope struct {
	// Value is the caller's value, unexamined. Absent on a tombstone.
	Value json.RawMessage `json:"v,omitempty"`

	// Fresh is when it stops being fresh, in Unix milliseconds. Zero means it is
	// fresh for as long as it exists, which is the behavior when no stale window
	// was asked for.
	Fresh int64 `json:"f,omitempty"`

	// Void marks a remembered absence: the loader was asked and said this does
	// not exist. It is a field rather than a nil Value so that a caller who
	// genuinely cached a JSON null is not mistaken for one.
	Void bool `json:"x,omitempty"`
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

// packVoid frames a remembered absence.
func packVoid() ([]byte, error) {
	out, err := json.Marshal(envelope{Void: true})
	if err != nil {
		return nil, fmt.Errorf("cache: cannot frame an absence: %w", err)
	}
	return out, nil
}

// unpacked is what one stored frame says: the value, whether it is past its
// freshness deadline, and whether it is a remembered absence.
type unpacked struct {
	value json.RawMessage
	stale bool
	void  bool
}

// unpack reads a stored frame.
func unpack(body []byte) (unpacked, error) {
	var e envelope
	if err := json.Unmarshal(body, &e); err != nil {
		return unpacked{}, fmt.Errorf("cache: cannot read the stored frame: %w", err)
	}
	if e.Void {
		return unpacked{void: true}, nil
	}
	if e.Fresh == 0 {
		return unpacked{value: e.Value}, nil
	}
	return unpacked{value: e.Value, stale: time.Now().UnixMilli() > e.Fresh}, nil
}
