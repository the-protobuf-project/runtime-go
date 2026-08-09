package core

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Keyspace builds the key names a strategy uses.
//
// Four strategies share one database, so each gets a segment of its own and no
// two can collide. That segment is also what makes the split cheap to reason
// about: an entry written through Volatile is invisible to Document, not by
// convention but because the name it lives under is different.
//
// Writing {head} for the prefix and namespace an operation was selected under:
//
//	{head}cache:doc:entry:{id}          a Document value
//	{head}cache:doc:index               the set of Document ids
//	{head}cache:vol:{key}               a Volatile value, nothing tracking it
//	{head}cache:idx:entry:{id}          an Indexed value
//	{head}cache:idx:index               the set of Indexed ids
//	{head}cache:idx:by:{field}:{value}  the set of ids filed under a field
//	{head}cache:idx:fields:{id}         the field=value pairs an id was filed under
//	{head}cache:aside:entry:{id}        a read-through value, or a remembered absence
//	{head}cache:aside:lock:{id}         the single-flight lock for one load
type Keyspace struct {
	base string
}

// NewKeyspace builds the layout for one prefix, namespace and database.
//
// Three things can qualify a key and each answers a different question. The
// prefix separates one program's caches from another's in a shared server. The
// namespace is the named database, and is the only one of the three that means
// the same thing on every backend. The index goes in only where the backend has
// no databases of its own and has to fake them — where it is already a property
// of the connection, repeating it would make keys longer and lookups no safer.
func NewKeyspace(prefix, namespace string, db int, embedDB bool) Keyspace {
	base := ""
	if prefix != "" {
		base = prefix + ":"
	}
	if namespace != "" {
		base += namespace + ":"
	}
	if embedDB {
		base += "db" + strconv.Itoa(db) + ":"
	}
	return Keyspace{base: base + "cache:"}
}

// CheckNamespace reports whether name can be used as a database name.
//
// The ban on a colon keeps the head of a key unambiguous. Prefix and name are
// joined with the separator, so allowing one inside a name makes the join
// lossy: prefix "app" with name "orders" and prefix "" with name "app:orders"
// both address app:orders:cache:, and two configurations that were never meant
// to meet share a keyspace. Rejecting the character is cheaper than every
// caller reasoning about it.
//
// It does not stop a name from reaching another strategy's keys — the literal
// cache: segment sits between the two, so that was never possible.
func CheckNamespace(name string) error {
	if name == "" {
		return errors.New("database name cannot be empty")
	}
	if strings.Contains(name, ":") {
		return fmt.Errorf(
			"database name %q cannot contain ':': it joins the prefix to the name, so allowing one would let two different configurations address the same keys",
			name)
	}
	return nil
}

// CheckKnown reports whether name is in the allowlist a cache was configured
// with. An empty allowlist accepts anything.
//
// The message lists what was allowed, because the whole point of the check is a
// typo and the fix is almost always visible next to the mistake.
func CheckKnown(name string, known []string) error {
	if len(known) == 0 {
		return nil
	}
	if slices.Contains(known, name) {
		return nil
	}
	return fmt.Errorf("database %q is not one of the configured databases %v", name, known)
}

// Head is the key prefix every key of one database begins with, which is what
// makes dropping one possible at all.
func (k Keyspace) Head() string { return k.base }

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

// pattern qualifies a caller's glob with this strategy's segment, so a scan
// cannot wander into another strategy's keys or another cache's prefix.
func (k Keyspace) pattern(glob string) string { return k.base + glob }
