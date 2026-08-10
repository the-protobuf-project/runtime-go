package redis

// scanBatch is the cursor hint for walking an id set. Large enough that the
// round trips are not the cost, small enough that one reply is not.
const scanBatch = 256

// Option configures a driver or a provider.
type Option func(*config)

type config struct {
	prefix string
}

// WithPrefix namespaces every key this driver reads and writes.
//
// Use it to run several independent stores against one Redis database, or to
// share a database with another concern — a cache or a stream. It is separate
// from the database selected by [Provider.SetDatabase]: the prefix separates one
// program's data from another's, the database separates one tenant's from
// another's, and conflating them would make a program unable to have both.
func WithPrefix(prefix string) Option {
	return func(c *config) { c.prefix = prefix }
}

func newConfig(opts ...Option) config {
	var c config
	for _, opt := range opts {
		if opt != nil {
			opt(&c)
		}
	}
	return c
}
