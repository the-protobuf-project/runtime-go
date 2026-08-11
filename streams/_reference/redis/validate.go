package redis

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/machanirobotics/loom/go/redis/options"
	"github.com/machanirobotics/loom/go/redis/shared"
	"github.com/machanirobotics/loom/go/ulid"
)

// processOptions resolves the final Redis configuration by taking an optional set of
// base options and overriding them with environment variables and defaults where needed.
func validateOptions(opts ...options.RedisClientOptions) (*options.RedisClientOptions, error) {
	baseOpts := options.RedisClientOptions{}
	if len(opts) > 0 {
		if len(opts) > 1 {
			shared.Pulse.Logger.Warn("Multiple Redis client options provided, using the first one.")
		}
		baseOpts = opts[0]
	}

	// Resolve the final options using the priority: baseOpts > Env > Default
	return resolveOptions(baseOpts), nil
}

func resolveOptions(opts options.RedisClientOptions) *options.RedisClientOptions {
	opts.Address = SetEnvOrDefault(&opts.Address, "REDIS_HOST", "localhost")
	opts.Port = SetEnvOrDefault(&opts.Port, "REDIS_PORT", "6379")
	opts.Username = SetEnvOrDefault(&opts.Username, "REDIS_USERNAME", "")
	opts.Password = SetEnvOrDefault(&opts.Password, "REDIS_PASSWORD", "")
	opts.Database = SetEnvOrDefault(&opts.Database, "REDIS_DATABASE", "0")
	return &opts
}

// If the variable is unset, it returns an empty string in Development mode.
// In other environments, it returns the first provided default value if one
// is given, or an empty string if no default is provided.
func SetEnvOrDefault(target *string, envVar string, defaultValue ...string) string {
	if *target == "" {
		if val := os.Getenv(envVar); val != "" {
			return val
		}
		if len(defaultValue) > 0 {
			return defaultValue[0]
		}
		return ""
	}
	return *target
}

// validateStreamSubjects checks if a given subject exists in the list of subjects.
// Returns an error if the subject is not found.
func validateStreamSubjects(subjects []string, subject string) error {
	if slices.Contains(subjects, subject) {
		return nil
	} else {
		_ = shared.Pulse.Logger.Errorf("subject not found")
		return fmt.Errorf("subject not found")
	}
}

// EnsureDBExists checks if a named database mapping exists and returns its numeric ID.
func (c *Client) ensureDBExists(name string) (int, error) {
	shared.Pulse.Logger.Debugf("EnsureDBExists called with name=%q", name)

	if c == nil || c.redisClient == nil {
		return 0, fmt.Errorf("redis client is not initialized")
	}
	if strings.TrimSpace(name) == "" {
		return 0, fmt.Errorf("database name cannot be empty")
	}

	dbs, err := c.ListDatabases()
	if err != nil {
		return 0, fmt.Errorf("failed to list databases: %w", err)
	}
	id, ok := dbs[name]
	if !ok {
		return 0, fmt.Errorf("database %q does not exist", name)
	}

	shared.Pulse.Logger.Debugf("EnsureDBExists: found ID=%d for name=%q", id, name)
	return id, nil
}

// notFoundIfNil is a utility that translates a redis.Nil error (key not found)
// into a more application-specific "document not found" error. For any other
// error, it returns it unchanged.
func (c *Client) notFoundIfNil(key, id string, err error) error {
	if err != nil {
		return fmt.Errorf("document key: %s Id:%s not found", key, id)
	}
	return err
}

type hashResult struct {
	genKey         string
	hash           string
	normalisedJson []byte
}

func (c *kvHandler) verifyHash(document Document) (*hashResult, error) {

	rdb := c.client.redisClient
	_, asMap, err := normalizeToJSON(document.Data)
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("failed to normalize document data: %v", err)
		return nil, fmt.Errorf("failed to normalize document data: %w", err)
	}
	hash, normalisedJson, err := hashContent(asMap)
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("failed to hash document content: %v", err)
		return nil, fmt.Errorf("failed to hash document content: %w", err)
	}
	genKey := ulid.Generate().GetRandomCode()
	document.SetID(genKey)
	ok, err := c.client.reserveHash(context.Background(), hash, genKey)
	if err != nil {
		_ = shared.Pulse.Logger.Errorf("failed to reserve hash %s: %v", hash, err)
		return nil, fmt.Errorf("failed to reserve hash: %w", err)
	}
	if !ok {
		// Duplicate detected - return existing document
		shared.Pulse.Logger.Debugf("duplicate detected (hash=%s), returning existing", hash)
		resp, found, err := c.client.loadExistingByHash(context.Background(), hash)
		if err != nil {
			return nil, err
		}
		if found {
			res, _, err := normalizeToJSON(resp.Data)
			if err != nil {
				return nil, err
			}
			return &hashResult{
				genKey:         resp.ID(),
				hash:           hash,
				normalisedJson: res,
			}, nil
		}

		// Rare race condition: hash reserved but document missing
		shared.Pulse.Logger.Warnf("hash reserved but doc missing, cleaning and retrying")
		if err := rdb.Del(context.Background(), kHash(hash)).Err(); err != nil {
			shared.Pulse.Logger.Warnf("failed to cleanup hash %s: %v", hash, err)
		}
		ok, err = c.client.reserveHash(context.Background(), hash, genKey)
		if err != nil || !ok {
			return nil, fmt.Errorf("failed to reserve hash after cleanup: %w", err)
		}
	}
	return &hashResult{
		genKey:         genKey,
		hash:           hash,
		normalisedJson: normalisedJson,
	}, nil
}
