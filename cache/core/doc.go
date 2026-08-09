// Package core implements every cache strategy once, over a [Driver].
//
// The four strategies in the parent package are algorithms, not storage. Getting
// an entry means building a key, fetching bytes, and decoding them; enumerating
// means reading an index, dropping the members whose entries have expired, and
// sweeping those members as you go. None of that changes when the bytes land in
// Redis instead of Memcache — only the eight primitives underneath do.
//
// So a backend implements [Driver] and gets [Document], [Volatile], [Indexed]
// and [Aside] for free. It writes no strategy code, which also means it cannot
// get the sweep, the refile, or the single-flight lock subtly wrong in its own
// particular way. When a fix lands here, every backend has it.
//
// # Capabilities
//
// Backends are not equally capable, and pretending otherwise is how a contract
// starts lying. Beyond the required [Driver] there are three optional
// interfaces — [Sets], [Leases], [Scanner] — that a driver implements if it can.
// Core type-asserts for them once, at construction, and the strategies that need
// a missing one report cache.ErrUnsupported rather than approximating it.
//
// That is the whole extension model. Adding a backend is a driver and a client;
// adding a capability to a backend is one more interface on the same type.
//
// # Ordering, not transactions
//
// An entry and its index member are two writes, and core does not assume a
// backend can make them one. It orders them instead: the index member is written
// first, so a failure in between leaves an index naming an entry that is not
// there — which is the case the sweep already handles on every read, because
// entries expire on their own and leave members behind regardless.
//
// The other order would leave an entry no listing can see and no group delete
// can reach, which nothing cleans up. A driver free to batch the pair atomically
// is welcome to; the algorithm here does not need it to.
package core
