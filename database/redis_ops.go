package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	goredis "github.com/redis/go-redis/v9"
	"github.com/the-protobuf-project/runtime-go/ulid"
)

// Create stores a document, deduplicating by content.
//
// The content hash is reserved with SETNX before the body is written, so two
// writers racing on identical content cannot both win. If the hash is already
// held, the document that holds it is returned instead of storing a copy — so
// a caller can tell a fresh write from a deduplicated one by comparing the
// returned ID against the one it supplied.
func (s *redisStore) Create(ctx context.Context, doc Document) (*Document, error) {
	hash, canonical, err := canonicalize(doc.Data)
	if err != nil {
		return nil, err
	}

	id := doc.ID()
	if id == "" {
		id = ulid.Generate().GetRandomCode()
	}

	reserved, err := s.rdb.SetNX(ctx, s.keys.byContent(hash), id, 0).Result()
	if err != nil {
		return nil, fmt.Errorf("database: failed to reserve content hash: %w", err)
	}

	if !reserved {
		// Someone else holds this content. Return their document rather than
		// storing a second copy.
		existing, err := s.rdb.Get(ctx, s.keys.byContent(hash)).Result()
		if err != nil {
			return nil, fmt.Errorf("database: failed to read content owner: %w", err)
		}
		body, err := s.readDoc(ctx, existing)
		if err == nil {
			out := NewDocument(existing, body)
			return &out, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}

		// The hash is reserved but its document is gone — a write that failed
		// between reserving and storing. Clear the orphaned reservation and
		// claim it, rather than refusing writes for content nobody holds.
		if delErr := s.rdb.Del(ctx, s.keys.byContent(hash)).Err(); delErr != nil {
			return nil, fmt.Errorf("database: failed to clear orphaned reservation: %w", delErr)
		}
		reserved, err = s.rdb.SetNX(ctx, s.keys.byContent(hash), id, 0).Result()
		if err != nil {
			return nil, fmt.Errorf("database: failed to reserve content hash: %w", err)
		}
		if !reserved {
			// Another writer claimed it in between; theirs stands.
			return nil, fmt.Errorf("%w: content is held by another document", ErrDuplicate)
		}
	}

	// Write the body and both indexes together.
	pipe := s.rdb.TxPipeline()
	pipe.Set(ctx, s.keys.doc(id), canonical, 0)
	pipe.SAdd(ctx, s.keys.index(), id)
	pipe.Set(ctx, s.keys.contentOf(id), hash, 0)
	if _, err := pipe.Exec(ctx); err != nil {
		// Release only a reservation this call actually made. Rolling back
		// unconditionally would delete the hash of a *pre-existing* document on
		// the deduplicated path above, silently destroying its dedup index and
		// letting a later Create store a duplicate under a new ID.
		if delErr := s.rdb.Del(ctx, s.keys.byContent(hash)).Err(); delErr != nil {
			err = errors.Join(err, delErr)
		}
		return nil, fmt.Errorf("database: failed to create %s: %w", id, err)
	}

	doc.SetID(id)
	return &doc, nil
}

// Get retrieves a document by ID.
func (s *redisStore) Get(ctx context.Context, id string) (Document, error) {
	body, err := s.readDoc(ctx, id)
	if err != nil {
		return Document{}, err
	}
	return NewDocument(id, body), nil
}

// readDoc fetches and decodes a document body, reporting an absent key as
// [ErrNotFound] and leaving every other failure — a dropped
// connection, a timeout — unchanged, so a transport error is never misreported
// as a missing record.
func (s *redisStore) readDoc(ctx context.Context, id string) (any, error) {
	raw, err := s.rdb.Get(ctx, s.keys.doc(id)).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		return nil, fmt.Errorf("database: failed to read %s: %w", id, err)
	}

	var data any
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("database: stored document %s is not valid JSON: %w", id, err)
	}
	return data, nil
}

// Update replaces a document's content, keeping the content index consistent.
//
// It refuses content that already belongs to a different document, since that
// would leave two IDs claiming one hash and break the store's guarantee that
// identical content resolves to a single document.
func (s *redisStore) Update(ctx context.Context, id string, doc Document) error {
	hash, canonical, err := canonicalize(doc.Data)
	if err != nil {
		return err
	}

	exists, err := s.rdb.Exists(ctx, s.keys.doc(id)).Result()
	if err != nil {
		return fmt.Errorf("database: failed to check %s: %w", id, err)
	}
	if exists == 0 {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}

	// Reject content already held by someone else.
	owner, err := s.rdb.Get(ctx, s.keys.byContent(hash)).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return fmt.Errorf("database: failed to check content hash: %w", err)
	}
	if owner != "" && owner != id {
		return fmt.Errorf("%w: content already stored as %s", ErrDuplicate, owner)
	}

	// The hash the document holds now, so the stale reservation is released in
	// the same transaction that claims the new one.
	oldHash, err := s.rdb.Get(ctx, s.keys.contentOf(id)).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return fmt.Errorf("database: failed to read current content hash: %w", err)
	}

	pipe := s.rdb.TxPipeline()
	pipe.Set(ctx, s.keys.doc(id), canonical, 0)
	pipe.Set(ctx, s.keys.contentOf(id), hash, 0)
	pipe.Set(ctx, s.keys.byContent(hash), id, 0)
	if oldHash != "" && oldHash != hash {
		pipe.Del(ctx, s.keys.byContent(oldHash))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("database: failed to update %s: %w", id, err)
	}
	return nil
}

// Delete removes a document, its index member, and its content reservation.
//
// Unlike a cache, a missing record is reported rather than ignored: documents
// do not expire on their own, so asking to delete one that is not there means
// the caller's view of the store is wrong.
func (s *redisStore) Delete(ctx context.Context, id string) error {
	exists, err := s.rdb.Exists(ctx, s.keys.doc(id)).Result()
	if err != nil {
		return fmt.Errorf("database: failed to check %s: %w", id, err)
	}
	if exists == 0 {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}

	// Read the hash before the transaction; releasing it is what lets the same
	// content be stored again later.
	hash, err := s.rdb.Get(ctx, s.keys.contentOf(id)).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return fmt.Errorf("database: failed to read content hash for %s: %w", id, err)
	}

	pipe := s.rdb.TxPipeline()
	pipe.Del(ctx, s.keys.doc(id))
	pipe.SRem(ctx, s.keys.index(), id)
	pipe.Del(ctx, s.keys.contentOf(id))
	if hash != "" {
		pipe.Del(ctx, s.keys.byContent(hash))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("database: failed to delete %s: %w", id, err)
	}
	return nil
}

// List returns stored documents, sorted by ID and narrowed by q.
//
// Redis sets have no order, so the IDs are sorted before Limit and Offset are
// applied — without that, paging would return overlapping or missing documents
// between calls.
func (s *redisStore) List(ctx context.Context, q Query) ([]Document, error) {
	ids, err := s.rdb.SMembers(ctx, s.keys.index()).Result()
	if err != nil {
		return nil, fmt.Errorf("database: failed to read index: %w", err)
	}
	slices.Sort(ids)

	if q.Offset > 0 {
		if q.Offset >= len(ids) {
			return []Document{}, nil
		}
		ids = ids[q.Offset:]
	}
	if q.Limit > 0 && q.Limit < len(ids) {
		ids = ids[:q.Limit]
	}

	docs := make([]Document, 0, len(ids))
	for _, id := range ids {
		body, err := s.readDoc(ctx, id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				// The body is gone but the index member survived — a delete
				// that failed partway. Drop the stale member; a cleanup failure
				// is not worth failing the read over.
				_ = s.rdb.SRem(ctx, s.keys.index(), id).Err()
				continue
			}
			return nil, err
		}
		docs = append(docs, NewDocument(id, body))
	}
	return docs, nil
}
