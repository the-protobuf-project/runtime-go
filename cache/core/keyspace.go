package core

import "strconv"

// Keyspace builds the key names a strategy uses.
//
// Four strategies share one database, so each gets a segment of its own and no
// two can collide. That segment is also what makes the split cheap to reason
// about: an entry written through Volatile is invisible to Document, not by
// convention but because the name it lives under is different.
//
//	{prefix}:cache:doc:entry:{id}          a Document value
//	{prefix}:cache:doc:index               the set of Document ids
//	{prefix}:cache:vol:{key}               a Volatile value, nothing tracking it
//	{prefix}:cache:idx:entry:{id}          an Indexed value
//	{prefix}:cache:idx:index               the set of Indexed ids
//	{prefix}:cache:idx:by:{field}:{value}  the set of ids filed under a field
//	{prefix}:cache:idx:fields:{id}         the field=value pairs an id was filed under
//	{prefix}:cache:aside:entry:{id}        a read-through value
//	{prefix}:cache:aside:lock:{id}         the single-flight lock for one load
//	{prefix}:cache:aside:void:{id}         a remembered absence
type Keyspace struct {
	base string
}

// NewKeyspace builds the layout for one prefix and database.
//
// The database index goes into the key on a backend that has no databases of its
// own, and stays out where it is already a property of the connection —
// repeating it there would only make keys longer and lookups no safer.
func NewKeyspace(prefix string, db int, embedDB bool) Keyspace {
	base := ""
	if prefix != "" {
		base = prefix + ":"
	}
	if embedDB {
		base += "db" + strconv.Itoa(db) + ":"
	}
	return Keyspace{base: base + "cache:"}
}

// Strategy narrows a keyspace to one strategy's segment.
func (k Keyspace) Strategy(name string) Keyspace {
	return Keyspace{base: k.base + name + ":"}
}

// entry holds a stored value.
func (k Keyspace) entry(id string) string { return k.base + "entry:" + id }

// index is the set of every id this strategy holds.
//
// No backend here can enumerate a logical group without walking its whole
// keyspace, so a strategy that promises enumeration maintains this itself.
// Entries expire on their own but leave their member behind, which is why reads
// sweep as they go.
func (k Keyspace) index() string { return k.base + "index" }

// by is the set of ids filed under one field value.
func (k Keyspace) by(field, value string) string {
	return k.base + "by:" + field + ":" + value
}

// fields records which field=value pairs an id was filed under, so deleting or
// refiling the entry can find the indexes naming it. Without it, cleanup would
// mean reading every index in the database.
func (k Keyspace) fields(id string) string { return k.base + "fields:" + id }

// raw holds a value under a key the caller named itself, used by Volatile where
// there is no generated id and no entry: prefix to keep out of.
func (k Keyspace) raw(name string) string { return k.base + name }

// lock is the single-flight lock for one id.
func (k Keyspace) lock(id string) string { return k.base + "lock:" + id }

// void remembers that an id does not exist.
func (k Keyspace) void(id string) string { return k.base + "void:" + id }

// pattern qualifies a caller's glob with this strategy's segment, so a scan
// cannot wander into another strategy's keys or another cache's prefix.
func (k Keyspace) pattern(glob string) string { return k.base + glob }
