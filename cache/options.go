package cache

import "time"

// Options are the per-operation settings a provider understands. A provider
// ignores what it cannot honor, so a call written against one backend still
// compiles and runs against another.
type Options struct {
	// TTL is how long the entry stays fresh. Zero falls back to
	// [Config.DefaultTTL].
	TTL time.Duration

	// Stale is how much longer past TTL an entry may still be served while it is
	// refreshed in the background. Zero means it may not: readers block on the
	// loader when it expires.
	Stale time.Duration

	// Indexes are the secondary keys to file this entry under, read by [Indexed]
	// and ignored by every other strategy.
	Indexes map[string]string
}

// Option configures one operation.
//
// Settings are options rather than parameters so a provider can gain or lose a
// capability without changing any signature — which is the point when several
// caches implement this contract.
type Option func(*Options)

// TTL sets how long an entry stays fresh.
func TTL(d time.Duration) Option {
	return func(o *Options) { o.TTL = d }
}

// Stale lets [Aside] serve an expired entry for d longer while it refreshes in
// the background, so a reader arriving after the TTL runs out gets slightly old
// data immediately instead of waiting for the loader.
//
// It is the difference between a cache that absorbs a traffic spike and one that
// forwards it: without this, every reader blocks the moment a hot key expires,
// which is exactly when the thing behind the cache can least afford them.
// Off by default, because serving stale data has to be the caller's choice.
func Stale(d time.Duration) Option {
	return func(o *Options) { o.Stale = d }
}

// Index files an entry under a secondary key, so [Indexed.ByIndex] can find it
// by something other than its id. Repeat it for several fields.
func Index(field, value string) Option {
	return func(o *Options) {
		if o.Indexes == nil {
			o.Indexes = make(map[string]string, 1)
		}
		o.Indexes[field] = value
	}
}

// NewOptions folds opts into a single Options, applying def as the TTL when no
// option named one. Implementations call this rather than re-implementing the
// loop.
func NewOptions(def Options, opts ...Option) Options {
	o := def
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
