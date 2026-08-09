package redis

// keys builds the Redis key names this store uses, namespaced by an optional
// prefix so several stores can share one database without colliding.
type keys struct {
	base string
}

func newKeys(prefix string) keys {
	if prefix != "" {
		prefix += ":"
	}
	return keys{base: prefix + "kv:"}
}

// entry holds a stored record.
func (k keys) entry(id string) string { return k.base + "entry:" + id }

// index is the set of every id this store holds. Records never expire, so
// unlike a cache index this only changes on create and delete.
func (k keys) index() string { return k.base + "index" }

// byContent maps a content hash to the id holding it, which is what makes the
// store content-addressed: writing the same content twice finds the first
// record instead of storing a copy.
func (k keys) byContent(hash string) string { return k.base + "content:" + hash }

// contentOf maps an id back to the hash it holds, the reverse of
// [keys.byContent]. It lets a write release the old reservation without
// rehashing the stored value.
func (k keys) contentOf(id string) string { return k.base + "hash:" + id }
