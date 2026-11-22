// Package cdn purges what Cloudflare's CDN has cached for a zone, through the
// official cloudflare-go SDK.
//
// It is the other end of a cache: what a [cache.DB] stores the program reads,
// and what the CDN stores every visitor reads, so a write that changes a page
// needs both forgotten. A purge is how the second one is.
//
//	cf := cloudflare.NewClient(option.WithAPIToken(os.Getenv("CLOUDFLARE_API_TOKEN")))
//	edge := cdn.NewPurger(cf, zoneID)
//
//	edge.Tags(ctx, "book-"+id)                         // everything served with Cache-Tag: book-<id>
//	edge.URLs(ctx, "https://example.com/books/"+id)    // one page
//
// Tags are the form worth designing for. A response that carries
// Cache-Tag: book-42, author-7 can be dropped by either, so the code that
// changes a book need not know every URL the book appears under.
//
// # What a purge promises
//
// Cloudflare acknowledging a purge means it accepted the request, not that
// every location has dropped the content yet; propagation takes seconds. The
// API also rate-limits purges per account — five a minute for tags, hosts and
// prefixes on the free plan — so purge per write, not per read, and prefer one
// call with many tags over many calls with one.
//
// [cache.DB]: https://pkg.go.dev/github.com/the-protobuf-project/runtime-go/cache#DB
package cdn
