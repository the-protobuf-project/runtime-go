package core

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/the-protobuf-project/runtime-go/cache"
)

// unlockTimeout bounds releasing a lock on a context that may already be done.
const unlockTimeout = 2 * time.Second

// loadAndStore runs the loader and writes what it returns, returning the value
// so the caller decodes what was just written rather than reading it back.
//
// It is always called inside a [flight], so within this process exactly one
// goroutine per id is ever here.
func (s *aside) loadAndStore(ctx context.Context, id string, o cache.Options) ([]byte, error) {
	if release, joined, body := s.claim(ctx, id); joined {
		// Another process was already loading and has published. Nothing left to
		// do — and notably, no waiting was involved to find that out.
		return body, nil
	} else if release != nil {
		defer release()
	}

	value, err := s.load(ctx, id)
	if err != nil {
		if isNotFound(err) {
			// Remember the absence briefly. Invalidate clears it, so something
			// created a moment later does not stay invisible.
			_ = s.driver.Set(ctx, s.keys.void(id), []byte{1}, s.empty)
			return nil, fmt.Errorf("%w: %s", cache.ErrNotFound, id)
		}
		return nil, fmt.Errorf("cache: loading %s: %w", id, err)
	}

	fresh, hard := freshness(o)
	body, err := pack(value, fresh)
	if err != nil {
		return nil, err
	}
	if serr := s.driver.Set(ctx, s.keys.entry(id), body, hard); serr != nil {
		return nil, fmt.Errorf("cache: cannot store %s: %w", id, serr)
	}

	// The value as the caller will decode it, unwrapped from its frame.
	value2, _, err := unpack(body)
	return value2, err
}

// claim tries to become the process that loads this id.
//
// It reports a release to call when the claim was won, and the published value
// when another process had already finished. Losing the race is never a wait:
// one extra read settles it, and if that read still finds nothing this process
// loads too. At most one redundant load per process, and a lock that is stuck or
// unreachable costs a round trip rather than a stall.
func (s *aside) claim(ctx context.Context, id string) (release func(), joined bool, body []byte) {
	if s.fenced == nil {
		// No safe release, so no lock. In-process collapsing already bounds
		// loads by the number of processes, which is the same bound a lock
		// would give a single-process program.
		return nil, false, nil
	}

	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return nil, false, nil // a lock is an optimization; never fail for one
	}

	won, err := s.driver.Add(ctx, s.keys.lock(id), token, s.lease)
	if err != nil || !won {
		if body, _, rerr := s.read(ctx, id); rerr == nil {
			return nil, true, body
		}
		return nil, false, nil
	}
	return func() { s.unlock(ctx, id, token) }, false, nil
}

// unlock releases a lock this call owns, and only this call.
//
// Two details, both of which were bugs before. It deletes conditionally on the
// token, so a loader finishing after its lease expired cannot release the lock a
// second goroutine now holds. And it runs on a context detached from the
// caller's, so a canceled request still releases — otherwise an abandoned
// request wedges that id for everyone until the lease runs out.
func (s *aside) unlock(ctx context.Context, id string, token []byte) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unlockTimeout)
	defer cancel()
	_, _ = s.fenced.DeleteIf(ctx, s.keys.lock(id), token)
}

// freshness splits an option pair into the freshness deadline written into the
// value and the hard expiry handed to the backend.
//
// Without a stale window the two are the same and the entry simply expires. With
// one, the backend keeps it for TTL+Stale while the frame says it stopped being
// fresh at TTL — which is what lets a reader in between serve it and refresh it.
func freshness(o cache.Options) (fresh, hard time.Duration) {
	if o.TTL <= 0 {
		return 0, 0
	}
	if o.Stale <= 0 {
		return 0, o.TTL
	}
	return o.TTL, o.TTL + o.Stale
}
