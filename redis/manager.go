package redis

import (
	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/database"
	"github.com/the-protobuf-project/runtime-go/redis/internal/conn"
	"github.com/the-protobuf-project/runtime-go/streams"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// DBManager is everything reachable inside one named database.
//
// It is the middle of the chain: a client names a database, the database hands
// back this, and the handlers on it do the work.
//
//	mgr, _ := c.SetDatabase(ctx, "orders")
//	mgr.Document.Cache.Create(ctx, "", order, cache.TTL(time.Minute))
//	mgr.Channel.Stream.Create(ctx, streams.Stream{ … })
//
// The handlers are grouped by what they hold rather than by backend feature:
// Document for things you store and read back, Channel for things you send and
// receive. Both are struct fields, so the whole surface is visible from the
// manager without a call.
type DBManager struct {
	// Document holds the two storage handlers.
	Document *DocumentHandler

	// Channel holds the two messaging handlers.
	Channel *ChannelHandler

	conn *conn.Conn
	name string
	log  telemetry.Logger
}

// DocumentHandler groups the storage handlers.
//
// Both store values, and the difference is lifetime: Cache entries expire,
// KV records do not. They are separate handlers rather than one with a flag
// because a caller who reaches for a cache and a caller who reaches for a store
// want different guarantees, and the type should say which they got.
type DocumentHandler struct {
	// Cache is ephemeral, TTL-bound storage.
	Cache cache.Cache

	// KV is durable, content-addressed storage.
	KV database.Store
}

// ChannelHandler groups the messaging handlers.
//
// Both carry values between publishers and subscribers, and the difference is
// when: Stream delivers on publish, Notify delivers when a TTL expires.
type ChannelHandler struct {
	// Stream delivers immediately, over pub/sub.
	Stream streams.Streams

	// Notify delivers when a message's TTL expires, over keyspace events.
	//
	// This needs the server running with --notify-keyspace-events Ex; without
	// it, published notifications never fire.
	Notify streams.Streams
}

// newDBManager assembles the handlers over a bound connection.
func newDBManager(c *conn.Conn, prefix, name string, log telemetry.Logger, meter telemetry.Meter) *DBManager {
	return &DBManager{
		Document: &DocumentHandler{
			Cache: newCache(c, prefix, log.With(telemetry.Fields{"handler": "cache"}), meter),
			KV:    newKV(c, prefix, log.With(telemetry.Fields{"handler": "kv"}), meter),
		},
		Channel: &ChannelHandler{
			Stream: newStreams(c, prefix, kindStream, log.With(telemetry.Fields{"handler": "stream"}), meter),
			Notify: newStreams(c, prefix, kindNotify, log.With(telemetry.Fields{"handler": "notify"}), meter),
		},
		conn: c,
		name: name,
		log:  log,
	}
}

// Name returns the database this manager is bound to.
func (m *DBManager) Name() string { return m.name }

// Index returns the Redis database index this manager is bound to.
func (m *DBManager) Index() int { return m.conn.DB() }

// Close releases this manager's connection.
//
// A manager owns the connection [Client.SetDatabase] opened for it, so it must
// be closed even if the client that made it already was.
func (m *DBManager) Close() error {
	return m.conn.Close()
}
