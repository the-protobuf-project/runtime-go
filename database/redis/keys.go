package redis

import (
	"strings"

	"github.com/the-protobuf-project/runtime-go/database/store"
)

// keys builds every Redis key one driver uses.
//
// The prefix and the selected database are baked in at construction, so an
// operation asks for "the key for this record" and cannot reach another
// database's keyspace. Writing {head} for those two:
//
//	{head}{table}:rec:{key}          one record, proto wire format
//	{head}{table}:ids.z              the sorted set of primary keys, for List and Count
//	{head}{table}:uniq:{col}:{value} a reservation on a unique column, holding the key
//
// The table comes from the [store.Resource], so two resources never share a
// keyspace even under one prefix and one database.
type keys struct {
	head string
}

// newKeys builds the layout for one prefix and database.
//
// The database is a key segment because Redis's own numbered databases are a
// property of the connection: selecting one would mean a second client and a
// second pool, and would cap a multi-tenant program at sixteen tenants. A
// segment has no ceiling and needs no connection of its own.
func newKeys(prefix, database string) keys {
	head := ""
	if prefix != "" {
		head = prefix + ":"
	}
	if database != "" {
		head += database + ":"
	}
	return keys{head: head}
}

// table narrows the keyspace to one resource.
func (k keys) table(res *store.Resource) string {
	return k.head + res.Table + ":"
}

// record holds one message, encoded.
func (k keys) record(res *store.Resource, key string) string {
	return k.table(res) + "rec:" + key
}

// ids is the sorted set of primary keys this resource holds.
//
// The suffix records the type. It was a plain set under {table}:ids, and a
// sorted set answers the same questions in log time without reading every id
// into the client — but the two are different Redis types under one name, and
// an existing deployment would meet WRONGTYPE on its first write rather than a
// migration. A new name means the old key is simply unused: visible, droppable,
// and never half-read.
//
// Redis cannot enumerate a logical group without walking its whole keyspace, so
// a driver that promises List maintains this itself. It is the one piece of
// bookkeeping every write pays for.
func (k keys) ids(res *store.Resource) string {
	return k.table(res) + "ids.z"
}

// unique reserves one value of one unique column, holding the primary key that
// owns it.
//
// The value goes in the key rather than in a hash so the reservation can be
// claimed with SET NX — one atomic round trip, which is what makes two writers
// racing on the same value resolvable at all.
func (k keys) unique(res *store.Resource, column, value string) string {
	return k.table(res) + "uniq:" + column + ":" + value
}

// pattern matches every key belonging to one resource, for dropping it.
func (k keys) pattern(res *store.Resource) string {
	return k.table(res) + "*"
}

// escape makes a value safe to put in a key position.
//
// A colon in a value would otherwise let one column's reservation collide with
// another's — an e-mail column holding "a:b" and a name column holding "b"
// under a table whose columns are "a" and "name" reach the same key. Percent is
// escaped first so the encoding stays reversible, which matters only for
// reading a key by eye but costs nothing.
func escape(v string) string {
	v = strings.ReplaceAll(v, "%", "%25")
	return strings.ReplaceAll(v, ":", "%3A")
}
