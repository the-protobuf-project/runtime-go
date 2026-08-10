// Package redis implements the backend-agnostic store.Driver over Redis: durable
// records addressed by a [store.Resource]'s primary key, stored as proto wire
// format, with a set of keys alongside so they can be listed.
//
// It is the counterpart to database/orm — same contract, same descriptors,
// no query language. Reach for it where the access pattern is by key and the
// operational story is one fewer moving part than a relational database.
//
// # Records are proto bytes, not JSON
//
// Every other driver writes a resource's columns into something column-shaped.
// Redis stores opaque bytes, so the only question is the encoding, and JSON
// answers it wrongly in two ways that never announce themselves: a bytes column
// becomes base64 that decodes back as the base64 text, and a uint64 past 2^53
// becomes a float that comes back a different number. Proto wire format loses
// neither.
//
// # What it has and what it does not
//
// It implements [store.Migrator] and [store.Batcher], and not
// [store.Transactional] — Redis has no rollback across the several keys one
// write touches, so the capability is absent rather than approximated, and
// [store.DB.Tx] reports [store.ErrUnimplemented] naming the backend.
//
// Uniqueness is enforced here rather than by the server: a column the descriptor
// marks Unique gets a reservation key claimed with SET NX, moved on update and
// released on delete. A descriptor that says a column is unique while the store
// does not enforce it would be exactly the kind of quiet lie the rest of this
// contract exists to avoid.
//
// The index is a sorted set rather than a plain set, which is what lets a page
// be read in log time and cut by the server. A plain set has no order, so paging
// one means reading every id into the client to sort and slice it — a million
// records allocating a million strings to return fifty, once per concurrent
// caller. A page now costs the same whatever the table holds.
//
// List pages stably and orders by the primary key; opts.Filter and any other
// OrderBy are ignored, as [store.ListOptions] permits. That is not a gap to fill
// later — Redis has no query language, and filtering client-side would read the
// whole table to return ten rows.
//
// # Example
//
//	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
//	defer rdb.Close()
//
//	p := redis.NewProvider(rdb, redis.WithPrefix("app"))
//	db, _ := p.SetDatabase(ctx, "tenant_a")
//	defer db.Close()
//
//	db.Create(ctx, res, book)
//
// The client is yours: this package does not dial it and does not close it.
// Hand the same one to the cache and streams layers and all three share a pool.
package redis
