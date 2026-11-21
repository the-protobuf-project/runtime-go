package cdn

import (
	"context"
	"fmt"

	"github.com/cloudflare/cloudflare-go/v7"
	"github.com/cloudflare/cloudflare-go/v7/cache"
)

// perRequest is how many URLs, tags, hosts or prefixes go into one purge — the
// most every plan accepts. Longer lists are sent in pieces.
const perRequest = 100

// Purger drops cached content from one zone.
type Purger struct {
	client *cloudflare.Client
	zone   string
}

// NewPurger returns a purger for the zone. The client is yours: this package
// neither builds nor closes it.
func NewPurger(client *cloudflare.Client, zoneID string) *Purger {
	return &Purger{client: client, zone: zoneID}
}

// URLs purges the content cached for each full URL, such as
// https://example.com/css/site.css.
func (p *Purger) URLs(ctx context.Context, urls ...string) error {
	return p.each(ctx, "urls", urls, func(chunk []string) cache.CachePurgeParamsBodyUnion {
		return cache.CachePurgeParamsBodyCachePurgeSingleFile{Files: cloudflare.F(chunk)}
	})
}

// Tags purges everything served with one of these tags in its Cache-Tag header.
func (p *Purger) Tags(ctx context.Context, tags ...string) error {
	return p.each(ctx, "tags", tags, func(chunk []string) cache.CachePurgeParamsBodyUnion {
		return cache.CachePurgeParamsBodyCachePurgeFlexPurgeByTags{Tags: cloudflare.F(chunk)}
	})
}

// Hosts purges everything cached for these hostnames, such as www.example.com.
func (p *Purger) Hosts(ctx context.Context, hosts ...string) error {
	return p.each(ctx, "hosts", hosts, func(chunk []string) cache.CachePurgeParamsBodyUnion {
		return cache.CachePurgeParamsBodyCachePurgeFlexPurgeByHostnames{Hosts: cloudflare.F(chunk)}
	})
}

// Prefixes purges everything whose URL starts with one of these — a host and a
// path, such as www.example.com/blog/, with no scheme, query or fragment.
func (p *Purger) Prefixes(ctx context.Context, prefixes ...string) error {
	return p.each(ctx, "prefixes", prefixes, func(chunk []string) cache.CachePurgeParamsBodyUnion {
		return cache.CachePurgeParamsBodyCachePurgeFlexPurgeByPrefixes{Prefixes: cloudflare.F(chunk)}
	})
}

// Everything purges the whole zone. Every visitor's next request for anything
// goes to the origin, so this is for an incident or a deploy that changed
// everything, not for routine invalidation.
func (p *Purger) Everything(ctx context.Context) error {
	return p.send(ctx, "everything", cache.CachePurgeParamsBodyCachePurgeEverything{
		PurgeEverything: cloudflare.F(true),
	})
}

// each sends a list in pieces the API accepts. An empty list purges nothing
// rather than being sent: the API would refuse it, and nothing asked for is
// nothing to do.
func (p *Purger) each(ctx context.Context, what string, items []string, body func([]string) cache.CachePurgeParamsBodyUnion) error {
	for start := 0; start < len(items); start += perRequest {
		chunk := items[start:min(start+perRequest, len(items))]
		if err := p.send(ctx, what, body(chunk)); err != nil {
			return err
		}
	}
	return nil
}

func (p *Purger) send(ctx context.Context, what string, body cache.CachePurgeParamsBodyUnion) error {
	if p.client == nil || p.zone == "" {
		return fmt.Errorf("cdn: a purger needs a client and a zone id")
	}
	_, err := p.client.Cache.Purge(ctx, cache.CachePurgeParams{ZoneID: cloudflare.F(p.zone), Body: body})
	if err != nil {
		return fmt.Errorf("cdn: cannot purge %s in zone %s: %w", what, p.zone, err)
	}
	return nil
}
