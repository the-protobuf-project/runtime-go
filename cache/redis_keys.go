package cache

// keys builds the Redis key names for one cache, namespaced by an optional
// prefix. Two caches sharing a Redis database but configured with different
// prefixes do not collide.
//
// The prefix is stored with its separator already appended, so the key helpers
// stay a single concatenation and an empty prefix costs nothing.
type redisKeys struct {
	prefix string
}

func newRedisKeys(prefix string) redisKeys {
	if prefix != "" {
		prefix += ":"
	}
	return redisKeys{prefix: prefix}
}

// entry returns the key holding a cache entry's body.
// Data type: string (JSON).
func (k redisKeys) entry(id string) string { return k.prefix + "cache:" + id }

// index returns the set key tracking every cache entry ID.
//
// The index is maintained because Redis has no way to enumerate the members of
// a logical group without scanning the whole keyspace. Entries expire on their
// own but leave their index member behind, so List sweeps stale members as it
// reads them.
// Data type: set.
func (k redisKeys) index() string { return k.prefix + "caches:index" }
