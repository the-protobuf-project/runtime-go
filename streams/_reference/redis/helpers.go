package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/machanirobotics/loom/go/ulid"
)

// loadExistingByHash checks if a document with the given content hash already
// exists. If found, it loads and returns the document, preventing data duplication.
func (c *Client) loadExistingByHash(ctx context.Context, hash string) (Document, bool, error) {
	rdb := c.redisClient

	// Look up the ID associated with the content hash.
	existingID, err := rdb.Get(ctx, kHash(hash)).Result()
	if err != nil || existingID == "" {
		// No document with this hash exists - this is expected, not an error.
		return Document{}, false, nil //nolint:nilerr // Key not found is expected, not an error
	}

	// A document exists; load its content.
	// Assumes readDocAsMap is a helper defined elsewhere to fetch and parse the document.
	m, err := c.readDocAsMap(ctx, existingID)
	if err != nil {
		return Document{}, false, fmt.Errorf("failed to read existing doc %q: %w", existingID, err)
	}

	return Document{id: existingID, Data: m}, true, nil
}

// reserveHash attempts to atomically reserve a content hash for a new document ID
// using the SETNX (Set if Not Exists) command. This prevents race conditions where
// two processes might try to write the same content simultaneously.
func (c *Client) reserveHash(ctx context.Context, hash string, id string) (bool, error) {
	rdb := c.redisClient

	// SetNX is atomic. It will only set the key if it does not already exist.
	ok, err := rdb.SetNX(ctx, kHash(hash), id, 0).Result() //nolint:staticcheck // SetNX is clearer than Set with NX option for this use case

	if err != nil {
		return false, fmt.Errorf("failed to reserve hash: %w", err)
	}
	return ok, nil
}

// writeNewDoc writes all data for a new document within a single atomic transaction.
// This ensures that the document and all its associated indexes are created together
func (c *Client) writeNewDoc(ctx context.Context, id, hash string, canonJSON []byte) error {
	rdb := c.redisClient

	// Use a pipeline to execute commands atomically.
	pipe := rdb.TxPipeline()
	pipe.Set(ctx, kDoc(id), canonJSON, 0) // Set the document body.
	pipe.SAdd(ctx, kIndex, id)            // Add the ID to the main index set.
	pipe.Set(ctx, kDocHash(id), hash, 0)  // Set the ID-to-hash mapping.
	_, execErr := pipe.Exec(ctx)

	if execErr != nil {
		return fmt.Errorf("failed to execute new doc transaction: %w", execErr)
	}
	return nil
}

// Use SETNX instead of Incr to set db > 2, as we need the 0 and 1 for other purposes, like default db and stream db
func getNextID(ctx context.Context, rdb *Client) (int64, error) {
	key := kDBCounter
	initialValue := 1 // We set it to 1, so the first INCR makes it 2

	// 1. Atomically set the initial value IF the key doesn't exist.
	// The expiration is 0, meaning it never expires.
	_, err := rdb.redisClient.SetNX(ctx, key, initialValue, 0).Result() //nolint:staticcheck // SetNX is clearer than Set with NX option
	if err != nil {
		// Handle connection or other Redis errors during the SetNX attempt
		return 0, fmt.Errorf("SetNX error: %w", err)
	}

	// 2. Increment the value (atomically) and get the new value.If we just ran SetNX, the value was 1, so INCR results in 2.
	// If the key already existed, it increments from its current value.
	nextID, err := rdb.redisClient.Incr(ctx, key).Result()
	if err != nil {
		// Handle errors during the Incr attempt
		return 0, fmt.Errorf("incr error: %w", err)
	}

	return nextID, nil
}

func (c *StreamHandler) getStreamInfo(stream string) (*Stream, error) {
	ctx := context.Background()
	res, err := c.Client.redisClient.XRangeN(ctx, stream, "-", "+", 1).Result()
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, fmt.Errorf("stream info length is zero")
	}
	subs := res[0].Values[kStreamInfo]
	jsonStr, ok := subs.(string)

	if !ok {
		fmt.Println("stream-info is not a string:", subs)
		return nil, fmt.Errorf("stream-info is not a string: %v", subs)
	}
	var info Stream

	err = json.Unmarshal([]byte(jsonStr), &info)
	if err != nil {
		fmt.Println("JSON unmarshal error:", err)
		return nil, fmt.Errorf("failed to unmarshal stream info: %w", err)
	}
	info.SetID(stream)
	return &info, nil
}

func createStreamKey(req Stream) Stream {
	subjects := append([]string(nil), req.Subjects...)
	if req.ID() == "" {
		req.SetID(ulid.Generate().GetTimeCode() + ":" + req.Name)
	}
	return Stream{
		id:          req.ID(),
		Name:        req.Name,
		Description: req.Description,
		Subjects:    subjects,
		UserID:      req.UserID,
		Active:      true,
	}
}

func (n *NotifyHandler) getStream(key string) (*Stream, error) {
	ctx := context.Background()
	// We store metadata in the key itself as a stream message "notify-info"

	res, err := n.client.redisClient.XRangeN(ctx, key, "-", "+", 1).Result()
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, fmt.Errorf("length of response is 0")
	}

	subs := res[0].Values["notify-info"]
	jsonStr, ok := subs.(string)

	if !ok {
		fmt.Println("notify-info is not a string:", subs)
		return nil, fmt.Errorf("notify-info is not a string")
	}

	var info Stream
	err = json.Unmarshal([]byte(jsonStr), &info)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal notify info: %w", err)
	}
	info.SetID(key)
	return &info, nil
}
