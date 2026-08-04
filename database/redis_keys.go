package database

// keys builds the Redis key names for one store, namespaced by an optional
// prefix. Two stores sharing a Redis database but configured with different
// prefixes do not collide.
type redisKeys struct {
	prefix string
}

func newRedisKeys(prefix string) redisKeys {
	if prefix != "" {
		prefix += ":"
	}
	return redisKeys{prefix: prefix}
}

// doc returns the key holding a document's body.
// Data type: string (canonical JSON).
func (k redisKeys) doc(id string) string { return k.prefix + "doc:" + id }

// index returns the set key tracking every document ID. Documents never expire,
// so unlike the cache index this one only changes when a document is created or
// deleted.
// Data type: set.
func (k redisKeys) index() string { return k.prefix + "docs:index" }

// byContent returns the content-hash-to-ID key. This is what makes the store
// content-addressed: writing the same content twice finds the first document
// instead of storing a second copy.
// Data type: string.
func (k redisKeys) byContent(hash string) string { return k.prefix + "docs:content:" + hash }

// contentOf returns the ID-to-content-hash key, the reverse of [keys.byContent].
// It is what lets Update and Delete find and release the hash a document
// currently holds without rehashing its stored body.
// Data type: string.
func (k redisKeys) contentOf(id string) string { return k.prefix + "dochash:" + id }
