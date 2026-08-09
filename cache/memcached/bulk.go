package memcached

import (
	"context"

	"github.com/the-protobuf-project/runtime-go/cache/core"
)

// Bulk is the one optional capability this backend has. It changes nothing about
// what memcached can express and a great deal about what a read costs.
var (
	_ core.Driver = primitives{}
	_ core.Bulk   = primitives{}
)

// GetMany fetches many keys in one exchange.
//
// The reply is keyed rather than ordered, so this maps it back onto the order
// asked for and leaves a nil where a key was not there — a miss, not a failure.
func (p primitives) GetMany(_ context.Context, keys []string) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	found, err := p.client.GetMulti(keys)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(keys))
	for i, key := range keys {
		if item, ok := found[key]; ok {
			out[i] = item.Value
		}
	}
	return out, nil
}

// ExistsMany reports liveness for many keys in one exchange.
//
// It is the same multi-get with the values thrown away — there being no
// existence check, the payloads cross the network to answer a yes-or-no
// question. Far cheaper than one round trip per key, and still the reason a
// sweep here costs more than it looks like it should.
func (p primitives) ExistsMany(_ context.Context, keys []string) ([]bool, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	found, err := p.client.GetMulti(keys)
	if err != nil {
		return nil, err
	}
	out := make([]bool, len(keys))
	for i, key := range keys {
		_, out[i] = found[key]
	}
	return out, nil
}
