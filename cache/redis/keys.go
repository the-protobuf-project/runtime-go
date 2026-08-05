package redis

// keys builds the Redis key names this cache uses, namespaced by an optional
// prefix so several caches can share one database without colliding.
type keys struct {
	base string
}

func newKeys(prefix string) keys {
	if prefix != "" {
		prefix += ":"
	}
	return keys{base: prefix + "cache:"}
}

// entry holds a stored value.
func (k keys) entry(id string) string { return k.base + "entry:" + id }

// index is the set of every id this cache holds.
//
// Redis cannot enumerate a logical group without scanning the whole keyspace,
// so the cache maintains this itself. Entries expire on their own but leave
// their member behind, which is why reads sweep as they go.
func (k keys) index() string { return k.base + "index" }
