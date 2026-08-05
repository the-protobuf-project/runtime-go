// Package conn holds the Redis connection and the named-logical-database
// registry the provider is built on.
//
// It is internal because it is machinery, not contract: the operations above it
// (cache, kv, stream, notify) are what callers use, and they all reach the
// server through a Conn from here.
package conn
