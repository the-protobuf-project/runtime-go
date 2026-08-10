package arangodb

import (
	"strings"
)

// ArangoDB's key rules, the URL's, and what this package does about the overlap.
//
// Two constraints meet here and neither is negotiable. A document key may not
// contain a slash — that separates a collection from a key in the _id ArangoDB
// builds — and resource names in this runtime are AIP-shaped, so they routinely
// do. And a read passes the key in the URL path without the driver escaping it a
// second time, so anything the URL reserves has to be gone before it gets there.
//
// Percent-encoding is the obvious answer and is exactly wrong: a stored
// "users%2Fada" is requested as ".../users%2Fada", the server decodes the escape
// back to a slash, and the read lands on a different collection. The write
// succeeds and every read after it misses, which is the worst shape a bug can
// have.
//
// So the escape character is the dot — legal in an ArangoDB key, reserved by
// nothing in a URL path — and everything outside the set both agree on becomes a
// dot and two hex digits. A literal dot doubles.
//
//	users/ada  ->  users.2Fada
//	v1.2       ->  v1..2
//	100%       ->  100.25
//
// Keys stay legible in arangosh, which a wholesale base64 scheme would have
// cost.
const keyEscape = '.'

// keySafe reports whether a byte can appear in a key untouched: allowed by
// ArangoDB and reserved by nothing in a URL path segment. The dot is excluded
// because it is the escape.
func keySafe(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}
	return strings.IndexByte("_-:@()+,=;$!*'", c) >= 0
}

const hexDigits = "0123456789ABCDEF"

// escapeKey turns a caller's id into something ArangoDB will accept as a _key
// and the driver will round-trip through a URL.
func escapeKey(id string) string {
	clean := true
	for i := 0; i < len(id); i++ {
		if !keySafe(id[i]) {
			clean = false
			break
		}
	}
	if clean {
		return id
	}

	var b strings.Builder
	b.Grow(len(id) + 8)
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case keySafe(c):
			b.WriteByte(c)
		case c == keyEscape:
			b.WriteByte(keyEscape)
			b.WriteByte(keyEscape)
		default:
			b.WriteByte(keyEscape)
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&0x0f])
		}
	}
	return b.String()
}

// unescapeKey reverses [escapeKey], so a record comes back under the id it went
// in with rather than the one the server stored.
func unescapeKey(key string) string {
	if !strings.ContainsRune(key, keyEscape) {
		return key
	}
	var b strings.Builder
	b.Grow(len(key))
	for i := 0; i < len(key); i++ {
		if key[i] != keyEscape {
			b.WriteByte(key[i])
			continue
		}
		if i+1 < len(key) && key[i+1] == keyEscape {
			b.WriteByte(keyEscape)
			i++
			continue
		}
		if i+2 < len(key) {
			hi, hiOK := unhex(key[i+1])
			lo, loOK := unhex(key[i+2])
			if hiOK && loOK {
				b.WriteByte(hi<<4 | lo)
				i += 2
				continue
			}
		}
		// Not an escape this package wrote — a key from somewhere else. Keep it
		// as it is rather than dropping a byte.
		b.WriteByte(key[i])
	}
	return b.String()
}

func unhex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	}
	return 0, false
}

// documentID builds the _id ArangoDB uses to address a document from anywhere in
// the database, which is what an edge's _from and _to have to hold.
func documentID(collection, id string) string {
	return collection + "/" + escapeKey(id)
}

// splitDocumentID reverses [documentID], returning the collection and the
// caller's id.
func splitDocumentID(docID string) (collection, id string) {
	collection, key, found := strings.Cut(docID, "/")
	if !found {
		return "", unescapeKey(docID)
	}
	return collection, unescapeKey(key)
}
