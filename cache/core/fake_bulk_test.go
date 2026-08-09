package core

import (
	"bytes"
	"context"
)

// The optional capabilities the fake implements, kept apart from the eight
// required ones so each file stays readable.

func (f *fake) GetMany(_ context.Context, keys []string) ([][]byte, error) {
	f.getMany.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([][]byte, len(keys))
	for i, key := range keys {
		if body, ok := f.live(key); ok {
			out[i] = body
		}
	}
	return out, nil
}

func (f *fake) ExistsMany(_ context.Context, keys []string) ([]bool, error) {
	f.existsMany.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]bool, len(keys))
	for i, key := range keys {
		_, out[i] = f.live(key)
	}
	return out, nil
}

func (f *fake) DeleteIf(_ context.Context, key string, want []byte) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	body, ok := f.live(key)
	if !ok || !bytes.Equal(body, want) {
		return false, nil
	}
	delete(f.values, key)
	return true, nil
}

func (f *fake) SetAdd(_ context.Context, key string, members ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.sets[key] == nil {
		f.sets[key] = make(map[string]bool)
	}
	for _, m := range members {
		f.sets[key][m] = true
	}
	return nil
}

func (f *fake) SetRemove(_ context.Context, key string, members ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, m := range members {
		delete(f.sets[key], m)
	}
	return nil
}

func (f *fake) SetMembers(_ context.Context, key string) ([]string, error) {
	f.setMembers.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, 0, len(f.sets[key]))
	for m := range f.sets[key] {
		out = append(out, m)
	}
	return out, nil
}
