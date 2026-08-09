package core

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// fake is an in-memory driver that counts what is asked of it, so a test can
// assert how many round trips an operation costs rather than only that it
// returned the right answer. Round trips are the thing being fixed.
type fake struct {
	mu     sync.Mutex
	values map[string]fakeValue
	sets   map[string]map[string]bool

	gets, exists, adds, sets_, deletes atomic.Int64
	getMany, existsMany                atomic.Int64
	setMembers                         atomic.Int64
}

type fakeValue struct {
	body    []byte
	expires time.Time
}

func newFake() *fake {
	return &fake{values: make(map[string]fakeValue), sets: make(map[string]map[string]bool)}
}

func (f *fake) Name() string { return "fake" }

// live must be called with the lock held.
func (f *fake) live(key string) ([]byte, bool) {
	v, ok := f.values[key]
	if !ok {
		return nil, false
	}
	if !v.expires.IsZero() && time.Now().After(v.expires) {
		delete(f.values, key)
		return nil, false
	}
	return v.body, true
}

func (f *fake) put(key string, body []byte, ttl time.Duration) {
	v := fakeValue{body: bytes.Clone(body)}
	if ttl > 0 {
		v.expires = time.Now().Add(ttl)
	}
	f.values[key] = v
}

func (f *fake) Get(_ context.Context, key string) ([]byte, error) {
	f.gets.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()

	body, ok := f.live(key)
	if !ok {
		return nil, ErrMiss
	}
	return body, nil
}

func (f *fake) Set(_ context.Context, key string, body []byte, ttl time.Duration) error {
	f.sets_.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()

	f.put(key, body, ttl)
	return nil
}

func (f *fake) Add(_ context.Context, key string, body []byte, ttl time.Duration) (bool, error) {
	f.adds.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.live(key); ok {
		return false, nil
	}
	f.put(key, body, ttl)
	return true, nil
}

func (f *fake) Replace(_ context.Context, key string, body []byte, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.live(key); !ok {
		return false, nil
	}
	f.put(key, body, ttl)
	return true, nil
}

func (f *fake) Delete(_ context.Context, keys ...string) error {
	f.deletes.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, key := range keys {
		delete(f.values, key)
	}
	return nil
}

func (f *fake) Exists(_ context.Context, key string) (bool, error) {
	f.exists.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()

	_, ok := f.live(key)
	return ok, nil
}

func (f *fake) Touch(_ context.Context, key string, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	body, ok := f.live(key)
	if !ok {
		return ErrMiss
	}
	f.put(key, body, ttl)
	return nil
}
