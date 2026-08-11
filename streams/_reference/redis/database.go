package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/machanirobotics/loom/go/redis/options"
	"github.com/machanirobotics/loom/go/redis/shared"
)

// CreateDatabase registers a new logical database with the given name.
// If a database with the same name already exists, it returns an error.
// A unique incremental ID is assigned using "db:counter" in Redis.
// The function also creates a reverse lookup key ("dbid:<id>").
func (d *Client) CreateDatabase(name string) error {
	ctx := context.Background()
	rdb := d.redisClient

	if rdb == nil {
		return shared.Pulse.Logger.Error("CreateDatabase: redis client not initialised")
	}

	// check for existing db
	exists, err := rdb.Get(ctx, kDB(name)).Result()
	if err != nil {
		_ = d.notFoundIfNil("", name, err)
	}

	if exists != "" {
		shared.Pulse.Logger.Warnf("CreateDatabase: database %q already exists", name)
		return fmt.Errorf("database %q already exists", name)
	}

	nextID, err := getNextID(ctx, d)
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("CreateDatabase: failed to increment db counter: %v", err)
		return fmt.Errorf("failed to get next database ID: %w", err)
	}

	err = rdb.Set(ctx, kDB(name), nextID, 0).Err()
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("CreateDatabase: failed to set database mapping: %v", err)
		return fmt.Errorf("failed to set database: %w", err)
	}
	if err := rdb.Set(context.Background(), kDBID(nextID), name, 0).Err(); err != nil {
		shared.Pulse.Logger.Warnf("CreateDatabase: failed to store reverse DB mapping: %v", err)
	}

	shared.Pulse.Logger.Debugf("CreateDatabase: created database %q with ID %d", name, nextID)

	return nil
}

// GetDatabase retrieves the database ID associated with the given name.
// If the database does not exist, an error is returned.
// The result is returned as a single-entry map: { name: id }.
func (d *Client) GetDatabase(name string) (map[string]int, error) {
	id, err := d.ensureDBExists(name)

	if err != nil {
		shared.Pulse.Logger.Warnf("GetDatabase: failed to find database %q: %v", name, err)
		return nil, err
	}
	return map[string]int{name: id}, nil
}

// SetDatabase selects an existing database by name and returns a RedisDBManager
// instance configured to operate on that database.
// If the database does not exist, an error is returned.
func (d *Client) SetDatabase(name string) (*RedisDBManager, error) {
	shared.Pulse.Logger.Debugf("SetDatabase called with name: %s", name)
	id, err := d.ensureDBExists(name)

	if err != nil {
		shared.Pulse.Logger.Warnf("SetDatabase: failed to find database %q: %v", name, err)
		return nil, err
	}
	client, err := d.buildClient(id)
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("SetDatabase: error building client for DB %d: %v", id, err)
		return nil, fmt.Errorf("failed to switch client: %w", err)
	}
	manager := &RedisDBManager{client: client, Document: GetDocumentHandler(client), Channel: GetChannelHandler(client)}
	return manager, nil
}

// DeleteDatabase removes the logical database mapping for the given name and
// flushes all keys stored inside the associated Redis DB.
// Deleting DB 0 is not permitted and results in an error.
// Both forward ("db:<name>") and reverse ("dbid:<id>") mappings are removed.
func (d *Client) DeleteDatabase(name string) error {
	shared.Pulse.Logger.Debugf("DeleteDatabase called for name: %s", name)

	id, err := d.ensureDBExists(name)

	if err != nil {
		_ = shared.Pulse.Logger.Errorf("DeleteDatabase: failed to find database %q: %v", name, err)
		return err
	}
	if id == 0 {
		return fmt.Errorf("delete database: deletion of default db 0 not allowed")
	}

	opts := d.redisClient.Options()
	tempClient, err := newClient(options.RedisClientOptions{
		Address:  opts.Addr,
		Username: opts.Username,
		Password: opts.Password,
		Database: strconv.Itoa(id),
	})
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("DeleteDatabase: error initializing temp client: %v", err)
		return fmt.Errorf("failed to initialize temp client: %w", err)
	}

	defer func() {
		if err := tempClient.Close(); err != nil {
			shared.Pulse.Logger.Warnf("DeleteDatabase: failed to close temp client: %v", err)
		}
	}()

	err = tempClient.redisClient.FlushDB(context.Background()).Err()
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("DeleteDatabase: failed to flush DB %d (%q): %v", id, name, err)
		return fmt.Errorf("failed to flush database %d: %w", id, err)
	}
	if err := d.redisClient.Del(context.Background(), kDB(name)).Err(); err != nil {
		_ = shared.Pulse.Logger.Errorf("DeleteDatabase: failed to delete mapping for %q: %v", name, err)
		return fmt.Errorf("failed to delete database mapping for %q: %w", name, err)
	}
	if err := d.redisClient.Del(context.Background(), kDBID(int64(id))).Err(); err != nil {
		shared.Pulse.Logger.Warnf("DeleteDatabase: failed to delete reverse mapping for %q: %v", name, err)
	}
	shared.Pulse.Logger.Debugf("DeleteDatabase: successfully deleted and flushed database %q (ID %d)", name, id)
	return nil
}

// ListDatabases returns all registered logical databases by scanning for keys
// matching the pattern "db:*" (excluding "db:counter").
// The returned map contains database names and their corresponding integer IDs.
func (d *Client) ListDatabases() (map[string]int, error) {
	ctx := context.Background()

	rdb := d.redisClient

	keys, err := rdb.Keys(ctx, "db:*").Result()
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("ListDatabases: failed to list keys: %v", err)
		return nil, fmt.Errorf("failed to retrieve database keys: %w", err)
	}

	databases := make(map[string]int)
	for _, key := range keys {
		if key == kDBCounter {
			continue
		}
		name := strings.TrimPrefix(key, "db:")
		idStr, err := rdb.Get(context.Background(), key).Result()
		if err != nil {
			shared.Pulse.Logger.Warnf("ListDatabases: failed to get ID for %q: %v", name, err)
			continue
		}
		id, err := strconv.Atoi(idStr)
		if err != nil {
			shared.Pulse.Logger.Warnf("ListDatabases: invalid ID %s for %q: %v", idStr, name, err)
			continue
		}
		databases[name] = id
	}

	return databases, nil
}
