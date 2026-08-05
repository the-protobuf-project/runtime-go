package conn

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// Registry keys. The name→index mapping lives in database 0 so every client can
// resolve a name regardless of which database it is currently pointed at.
const (
	keyDBPrefix   = "db:"
	keyDBIDPrefix = "dbid:"

	// registryDB is the database holding the name→index mapping.
	registryDB = 0

	// firstUserDB is the lowest index handed out to a named database.
	//
	// 0 holds the registry and 1 is reserved. Keeping the registry out of the
	// range it hands out means flushing a named database never destroys the
	// mapping that found it.
	firstUserDB = 2

	// fallbackDBCount is assumed when the server will not report its database
	// count — CONFIG GET is disabled on many managed instances. 16 is the Redis
	// default.
	fallbackDBCount = 16
)

// Options describe how to reach a Redis server.
type Options struct {
	Address  string
	Port     string
	Username string
	Password string

	// Database is the index to bind to. Empty means the registry database.
	Database string
}

// Addr renders the host:port this dials.
func (o Options) Addr() string {
	return o.Address + ":" + o.Port
}

// Conn is a connection bound to one Redis database.
//
// It owns the client it dialed and closes it in Close. A Conn produced by
// [Conn.Bind] owns its own client too — binding to another database opens a
// second connection rather than mutating this one, so two managers over
// different databases can never write into each other.
type Conn struct {
	rdb  *goredis.Client
	opts Options
	db   int
	log  telemetry.Logger
}

// Open dials a server and verifies it answers.
//
// The ping is deliberate: a Conn that cannot reach its server is worth failing
// at construction, where the caller still has a stack to report from, rather
// than on the first operation.
func Open(ctx context.Context, opts Options, log telemetry.Logger) (*Conn, error) {
	if log == nil {
		log = telemetry.NoopLogger
	}
	if opts.Address == "" {
		opts.Address = "localhost"
	}
	if opts.Port == "" {
		opts.Port = "6379"
	}
	if opts.Database == "" {
		opts.Database = strconv.Itoa(registryDB)
	}

	db, err := strconv.Atoi(opts.Database)
	if err != nil {
		return nil, fmt.Errorf("redis: database must be a number, got %q: %w", opts.Database, err)
	}

	log.Debug(ctx, "dialing redis", telemetry.Fields{"addr": opts.Addr(), "db": db})

	rdb := goredis.NewClient(&goredis.Options{
		Addr:     opts.Addr(),
		Username: opts.Username,
		Password: opts.Password,
		DB:       db,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		log.Error(ctx, "redis is unreachable", err, telemetry.Fields{"addr": opts.Addr(), "db": db})
		return nil, fmt.Errorf("redis: cannot reach %s (db %d): %w", opts.Addr(), db, err)
	}

	log.Info(ctx, "connected to redis", telemetry.Fields{"addr": opts.Addr(), "db": db})
	return &Conn{rdb: rdb, opts: opts, db: db, log: log}, nil
}

// Redis returns the underlying client for the operations to use.
func (c *Conn) Redis() *goredis.Client { return c.rdb }

// DB reports which database index this connection is bound to.
func (c *Conn) DB() int { return c.db }

// Log returns the logger this connection was opened with.
func (c *Conn) Log() telemetry.Logger { return c.log }

// Close releases the connection.
func (c *Conn) Close() error {
	if c == nil || c.rdb == nil {
		return nil
	}
	c.log.Debug(context.Background(), "closing redis connection",
		telemetry.Fields{"addr": c.opts.Addr(), "db": c.db})
	return c.rdb.Close()
}

// Bind opens a second connection to the same server, pointed at database index.
//
// It returns a new Conn rather than re-pointing this one. That is what lets
// several managers coexist: an earlier binding keeps working after a later one,
// which a single mutable connection could not offer.
func (c *Conn) Bind(ctx context.Context, index int) (*Conn, error) {
	opts := c.opts
	opts.Database = strconv.Itoa(index)

	c.log.Debug(ctx, "binding to database", telemetry.Fields{"from": c.db, "to": index})
	return Open(ctx, opts, c.log)
}

// CreateDatabase registers a name and assigns it the next free index.
//
// Registration is idempotent in effect but not silent: a name that already
// exists is reported, because the caller asked to create something and did not.
func (c *Conn) CreateDatabase(ctx context.Context, name string) (int, error) {
	if strings.TrimSpace(name) == "" {
		return 0, fmt.Errorf("redis: database name cannot be empty")
	}

	reg, err := c.registry(ctx)
	if err != nil {
		return 0, err
	}
	defer reg.release()

	c.log.Debug(ctx, "creating database", telemetry.Fields{"name": name})

	existing, err := reg.rdb.Get(ctx, keyDBPrefix+name).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		c.log.Error(ctx, "could not check for an existing database", err,
			telemetry.Fields{"name": name})
		return 0, fmt.Errorf("redis: cannot check database %q: %w", name, err)
	}
	if existing != "" {
		c.log.Warn(ctx, "database already exists", telemetry.Fields{"name": name, "index": existing})
		return 0, fmt.Errorf("redis: database %q already exists", name)
	}

	index, err := reg.claimIndex(ctx, name)
	if err != nil {
		c.log.Error(ctx, "could not allocate a database index", err, telemetry.Fields{"name": name})
		return 0, err
	}

	// The reverse mapping is already claimed; writing the forward one completes
	// the pair, so a lookup by name and a lookup by index agree.
	if err := reg.rdb.Set(ctx, keyDBPrefix+name, index, 0).Err(); err != nil {
		// Release the claim rather than stranding an index nobody can reach.
		_ = reg.rdb.Del(ctx, keyDBIDPrefix+strconv.Itoa(index)).Err()
		c.log.Error(ctx, "could not register the database", err,
			telemetry.Fields{"name": name, "index": index})
		return 0, fmt.Errorf("redis: cannot register database %q: %w", name, err)
	}

	c.log.Info(ctx, "created database", telemetry.Fields{"name": name, "index": index})
	return index, nil
}

// LookupDatabase resolves a name to its index.
func (c *Conn) LookupDatabase(ctx context.Context, name string) (int, error) {
	if strings.TrimSpace(name) == "" {
		return 0, fmt.Errorf("redis: database name cannot be empty")
	}

	reg, err := c.registry(ctx)
	if err != nil {
		return 0, err
	}
	defer reg.release()

	raw, err := reg.rdb.Get(ctx, keyDBPrefix+name).Result()
	if errors.Is(err, goredis.Nil) {
		c.log.Debug(ctx, "database not registered", telemetry.Fields{"name": name})
		return 0, fmt.Errorf("redis: database %q does not exist", name)
	}
	if err != nil {
		c.log.Error(ctx, "could not read the database registry", err, telemetry.Fields{"name": name})
		return 0, fmt.Errorf("redis: cannot read database %q: %w", name, err)
	}

	index, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("redis: database %q has a malformed index %q: %w", name, raw, err)
	}
	c.log.Debug(ctx, "resolved database", telemetry.Fields{"name": name, "index": index})
	return index, nil
}

// ListDatabases returns every registered name and its index.
func (c *Conn) ListDatabases(ctx context.Context) (map[string]int, error) {
	reg, err := c.registry(ctx)
	if err != nil {
		return nil, err
	}
	defer reg.release()

	out := make(map[string]int)
	var cursor uint64
	for {
		keys, next, err := reg.rdb.Scan(ctx, cursor, keyDBPrefix+"*", 100).Result()
		if err != nil {
			c.log.Error(ctx, "could not scan the database registry", err, nil)
			return nil, fmt.Errorf("redis: cannot list databases: %w", err)
		}

		for _, key := range keys {
			raw, err := reg.rdb.Get(ctx, key).Result()
			if err != nil {
				c.log.Warn(ctx, "skipping unreadable registry entry",
					telemetry.Fields{"key": key, "error": err.Error()})
				continue
			}
			index, err := strconv.Atoi(raw)
			if err != nil {
				c.log.Warn(ctx, "skipping registry entry with a malformed index",
					telemetry.Fields{"key": key, "value": raw})
				continue
			}
			out[strings.TrimPrefix(key, keyDBPrefix)] = index
		}

		cursor = next
		if cursor == 0 {
			break
		}
	}

	c.log.Debug(ctx, "listed databases", telemetry.Fields{"count": len(out)})
	return out, nil
}

// DeleteDatabase drops a name and flushes the database it pointed at.
//
// The registry database itself cannot be deleted: it holds the mapping that
// makes every other name resolvable.
func (c *Conn) DeleteDatabase(ctx context.Context, name string) error {
	index, err := c.LookupDatabase(ctx, name)
	if err != nil {
		return err
	}
	if index == registryDB {
		return fmt.Errorf("redis: database %q maps to the registry database and cannot be deleted", name)
	}

	c.log.Debug(ctx, "deleting database", telemetry.Fields{"name": name, "index": index})

	// Flush through a connection bound to that database — the current one may
	// be pointed somewhere else entirely.
	target, err := c.Bind(ctx, index)
	if err != nil {
		return fmt.Errorf("redis: cannot open database %q to flush it: %w", name, err)
	}
	defer func() { _ = target.Close() }()

	if ferr := target.rdb.FlushDB(ctx).Err(); ferr != nil {
		c.log.Error(ctx, "could not flush the database", ferr,
			telemetry.Fields{"name": name, "index": index})
		return fmt.Errorf("redis: cannot flush database %q: %w", name, ferr)
	}

	reg, err := c.registry(ctx)
	if err != nil {
		return err
	}
	defer reg.release()

	pipe := reg.rdb.TxPipeline()
	pipe.Del(ctx, keyDBPrefix+name)
	pipe.Del(ctx, keyDBIDPrefix+strconv.Itoa(index))
	if _, err := pipe.Exec(ctx); err != nil {
		c.log.Error(ctx, "flushed the database but could not drop its mapping", err,
			telemetry.Fields{"name": name, "index": index})
		return fmt.Errorf("redis: cannot drop the mapping for %q: %w", name, err)
	}

	c.log.Info(ctx, "deleted database", telemetry.Fields{"name": name, "index": index})
	return nil
}

// registryConn is a connection to the registry database, plus how to let it go.
type registryConn struct {
	rdb     *goredis.Client
	release func()
}

// registry returns a connection to the registry database.
//
// When this Conn is already bound there it is reused; otherwise a temporary one
// is opened and closed by release. Callers always defer release, so they do not
// have to know which case they got.
func (c *Conn) registry(ctx context.Context) (registryConn, error) {
	if c.db == registryDB {
		return registryConn{rdb: c.rdb, release: func() {}}, nil
	}

	tmp, err := c.Bind(ctx, registryDB)
	if err != nil {
		return registryConn{}, fmt.Errorf("redis: cannot reach the database registry: %w", err)
	}
	return registryConn{rdb: tmp.rdb, release: func() { _ = tmp.Close() }}, nil
}

// claimIndex takes the lowest free database index for name.
//
// A server has a fixed, small number of databases — 16 by default — so indices
// must be reusable: an ever-increasing counter would exhaust them after a
// handful of create/delete cycles, even though nothing is using them.
//
// The claim is a SETNX on the reverse mapping, so two callers racing on the
// same index cannot both win; the loser moves to the next one.
func (r registryConn) claimIndex(ctx context.Context, name string) (int, error) {
	max := r.databaseCount(ctx)

	for index := firstUserDB; index < max; index++ {
		won, err := SetNX(ctx, r.rdb, keyDBIDPrefix+strconv.Itoa(index), name)
		if err != nil {
			return 0, fmt.Errorf("redis: cannot claim a database index: %w", err)
		}
		if won {
			return index, nil
		}
	}

	return 0, fmt.Errorf(
		"redis: no free database index (this server has %d, %d are reserved, and the rest are in use)",
		max, firstUserDB)
}

// databaseCount reports how many databases the server has.
//
// CONFIG GET is disabled on many managed instances, so a failure falls back to
// the Redis default rather than refusing to allocate.
func (r registryConn) databaseCount(ctx context.Context) int {
	cfg, err := r.rdb.ConfigGet(ctx, "databases").Result()
	if err != nil {
		return fallbackDBCount
	}
	raw, ok := cfg["databases"]
	if !ok {
		return fallbackDBCount
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= firstUserDB {
		return fallbackDBCount
	}
	return n
}

// SetNX claims a key only if it is absent, reporting whether this caller won.
//
// go-redis deprecated its own SetNX in favor of SetArgs with Mode "NX"; the
// behavior is the same, and a nil reply means someone else already holds the
// key. It lives here so the operations above share one spelling of the claim.
func SetNX(ctx context.Context, rdb *goredis.Client, key, value string) (bool, error) {
	err := rdb.SetArgs(ctx, key, value, goredis.SetArgs{Mode: "NX"}).Err()
	if errors.Is(err, goredis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
