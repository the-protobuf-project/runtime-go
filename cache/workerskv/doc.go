// Package workerskv is the Cloudflare Workers KV backend, reached through the
// REST API by the official cloudflare-go SDK.
//
//	cf := cloudflare.NewClient(option.WithAPIToken(os.Getenv("CLOUDFLARE_API_TOKEN")))
//	client, err := workerskv.NewClient(ctx, workerskv.Config{
//	    Client: cf, AccountID: account, NamespaceID: namespace,
//	})
//
//	c := workerskv.New(client, cache.Config{Prefix: "example", DefaultTTL: time.Hour})
//	db, err := c.SetDatabase(ctx, "orders")
//
// KV is a store replicated to every Cloudflare location and read from the
// nearest one, which makes it very good at one thing — a value read often,
// everywhere, and written rarely — and shapes everything else about it.
//
// # Eventually consistent
//
// A write is visible at once where it was made and up to a minute later
// everywhere else, and a key written more than once a second is rate limited.
// No strategy here can paper over that, so the ones that would need it say so:
//
//   - Add and Replace refuse. KV has no conditional write, and a read followed
//     by a write is not one — under eventual consistency it is not even close.
//     Document.Update reports [cache.ErrUnsupported] for the same reason, and
//     read-through collapses loads within a process instead of locking across
//     them, because there is no conditional delete to release a lock with.
//   - Touch refuses. Extending a lease means rewriting the value, and a rewrite
//     racing a delete brings back what was deleted.
//
// Get, Set, Delete, the bulk reads, Scan and TTL all work.
//
// # Leases
//
// KV will not expire anything sooner than sixty seconds after it was written,
// so a shorter lease is lengthened to sixty seconds rather than refused: for a
// cache, an entry outliving its lease is a stale read, and refusing would make
// every short-lived value in a program unwritable here. A lease that must be
// shorter than a minute does not belong on this backend.
//
// # Names
//
// A database name is a key segment, exactly as on every other backend. The
// server enforces nothing between names; a namespace — KV's own word for a
// separate store — is the boundary it does enforce, and is chosen per client.
package workerskv
