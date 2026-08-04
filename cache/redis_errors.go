package cache

import (
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// notFound translates a redis.Nil reply (key absent) into [ErrNotFound],
// wrapped with the ID for context. Every other error — a dropped connection, a
// timeout, a WRONGTYPE reply — is returned unchanged, so a transport failure is
// never misreported as a missing key.
func notFound(id string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, goredis.Nil) {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return err
}
