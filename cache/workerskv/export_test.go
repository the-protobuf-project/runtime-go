package workerskv

import "context"

// GetMany reaches the bulk capability directly. Core only takes it on a backend
// with sets, which KV is not, so no strategy exercises it from outside.
func GetMany(ctx context.Context, c *Client, keys []string) ([][]byte, error) {
	return primitives{client: c}.GetMany(ctx, keys)
}
