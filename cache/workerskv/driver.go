package workerskv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cloudflare/cloudflare-go/v7"
	"github.com/cloudflare/cloudflare-go/v7/kv"
	"github.com/cloudflare/cloudflare-go/v7/shared"

	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/cache/core"
)

// backend names this driver in errors and telemetry.
const backend = "workerskv"

// Limits KV sets, which the driver works inside rather than reporting.
const (
	minTTL     = 60 * time.Second // the shortest expiry KV accepts
	bulkGet    = 100              // keys per bulk read
	bulkDelete = 10000            // keys per bulk delete
	listLimit  = 1000             // keys per listing page
)

// primitives is everything KV contributes: the required methods, and the
// optional capabilities it can honor — Bulk, Leases and Scanner.
//
// The absent ones are Sets, because KV has no server-side sets, and Fenced,
// because it has no conditional delete. Add, Replace and Touch are required, so
// they are here, and refuse; see the package documentation for why.
type primitives struct {
	client *Client
}

var (
	_ core.Driver  = primitives{}
	_ core.Bulk    = primitives{}
	_ core.Leases  = primitives{}
	_ core.Scanner = primitives{}
)

func (p primitives) Name() string { return backend }

// Get returns the stored bytes.
func (p primitives) Get(ctx context.Context, key string) ([]byte, error) {
	res, err := p.client.inner.KV.Namespaces.Values.Get(ctx, p.client.namespace, escape(key),
		kv.NamespaceValueGetParams{AccountID: cloudflare.F(p.client.account)})
	if err != nil {
		if missing(err) {
			return nil, core.ErrMiss
		}
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	return io.ReadAll(res.Body)
}

// Set writes unconditionally, lengthening a lease under [minTTL] to it.
func (p primitives) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	params := kv.NamespaceValueUpdateParams{
		AccountID: cloudflare.F(p.client.account),
		Value:     cloudflare.F[kv.NamespaceValueUpdateParamsValueUnion](shared.UnionString(value)),
	}
	if ttl > 0 {
		params.ExpirationTTL = cloudflare.F(expiry(ttl))
	}
	_, err := p.client.inner.KV.Namespaces.Values.Update(ctx, p.client.namespace, escape(key), params)
	return err
}

// Add refuses: KV has no write-if-absent.
func (p primitives) Add(context.Context, string, []byte, time.Duration) (bool, error) {
	return false, refuse("write only if a key is absent", "no conditional write")
}

// Replace refuses: KV has no write-if-present.
func (p primitives) Replace(context.Context, string, []byte, time.Duration) (bool, error) {
	return false, refuse("write only if a key is present", "no conditional write")
}

// Touch refuses: a lease is extended only by rewriting the value, and a rewrite
// racing a delete brings back what was deleted.
func (p primitives) Touch(context.Context, string, time.Duration) error {
	return refuse("extend a lease", "a lease changes only with the value")
}

// Delete removes keys in bulk. Removing an absent key is not an error.
func (p primitives) Delete(ctx context.Context, keys ...string) error {
	for start := 0; start < len(keys); start += bulkDelete {
		chunk := keys[start:min(start+bulkDelete, len(keys))]
		res, err := p.client.inner.KV.Namespaces.BulkDelete(ctx, p.client.namespace, kv.NamespaceBulkDeleteParams{
			AccountID: cloudflare.F(p.client.account),
			Body:      chunk,
		})
		if err != nil {
			return err
		}
		if res != nil && len(res.UnsuccessfulKeys) > 0 {
			return fmt.Errorf("workerskv: %d keys were not deleted, among them %q", len(res.UnsuccessfulKeys), res.UnsuccessfulKeys[0])
		}
	}
	return nil
}

// Exists reports whether a key is live.
//
// It is a read whose value is thrown away. KV's metadata endpoint looks cheaper
// but answers about a key's metadata rather than the key, and a wrong answer
// here costs more than the bytes.
func (p primitives) Exists(ctx context.Context, key string) (bool, error) {
	_, err := p.Get(ctx, key)
	if errors.Is(err, core.ErrMiss) {
		return false, nil
	}
	return err == nil, err
}

// GetMany reads keys a hundred at a time.
//
// The bulk endpoint returns text, which is what every strategy here stores; a
// value that is not valid UTF-8 would not survive it, and nothing in core
// writes one.
func (p primitives) GetMany(ctx context.Context, keys []string) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	out := make([][]byte, len(keys))
	for start := 0; start < len(keys); start += bulkGet {
		chunk := keys[start:min(start+bulkGet, len(keys))]
		res, err := p.client.inner.KV.Namespaces.BulkGet(ctx, p.client.namespace, kv.NamespaceBulkGetParams{
			AccountID: cloudflare.F(p.client.account),
			Keys:      cloudflare.F(chunk),
			Type:      cloudflare.F(kv.NamespaceBulkGetParamsTypeText),
		})
		if err != nil {
			return nil, err
		}
		// Read from the raw reply: the SDK types each value as a union, and a
		// missing key may be a null or may be absent — a pointer reads both.
		var body struct {
			Values map[string]*string `json:"values"`
		}
		if res != nil {
			if derr := json.Unmarshal([]byte(res.JSON.RawJSON()), &body); derr != nil {
				return nil, fmt.Errorf("workerskv: cannot read a bulk reply: %w", derr)
			}
		}
		for i, key := range chunk {
			if v := body.Values[key]; v != nil {
				out[start+i] = []byte(*v)
			}
		}
	}
	return out, nil
}

// ExistsMany reports liveness for many keys with the same bulk read.
func (p primitives) ExistsMany(ctx context.Context, keys []string) ([]bool, error) {
	values, err := p.GetMany(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := make([]bool, len(values))
	for i, v := range values {
		out[i] = v != nil
	}
	return out, nil
}

// TTL reports the remaining lease, read from a listing — the one place KV
// reports a key's expiry without its value. Zero is a key that never expires.
func (p primitives) TTL(ctx context.Context, key string) (time.Duration, error) {
	page, err := p.client.inner.KV.Namespaces.Keys.List(ctx, p.client.namespace, kv.NamespaceKeyListParams{
		AccountID: cloudflare.F(p.client.account),
		Prefix:    cloudflare.F(key),
		Limit:     cloudflare.F(float64(10)),
	})
	if err != nil {
		return 0, err
	}
	for i := range page.Result {
		k := &page.Result[i]
		if k.Name != key {
			continue
		}
		if k.Expiration == 0 {
			return 0, nil
		}
		left := time.Until(time.Unix(int64(k.Expiration), 0))
		if left <= 0 {
			return 0, core.ErrMiss
		}
		return left, nil
	}
	return 0, core.ErrMiss
}

// Scan lists the keys matching a glob.
//
// KV lists by prefix only, so the listing is cut by the glob's literal head and
// the rest of the pattern is matched here. A pattern that starts with a
// wildcard lists the whole namespace to answer.
func (p primitives) Scan(ctx context.Context, pattern string) ([]string, error) {
	prefix := pattern
	if i := strings.IndexAny(pattern, `*?[\`); i >= 0 {
		prefix = pattern[:i]
	}
	params := kv.NamespaceKeyListParams{AccountID: cloudflare.F(p.client.account), Limit: cloudflare.F(float64(listLimit))}
	if prefix != "" {
		params.Prefix = cloudflare.F(prefix)
	}
	var out []string
	pages := p.client.inner.KV.Namespaces.Keys.ListAutoPaging(ctx, p.client.namespace, params)
	for pages.Next() {
		name := pages.Current().Name
		ok, err := match(pattern, name)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, name)
		}
	}
	return out, pages.Err()
}

// match reports whether name matches a Redis-style glob: * any run, ? any one
// character, \ escaping the next. Character classes are refused rather than
// guessed at.
func match(pattern, name string) (bool, error) {
	p, n := []rune(pattern), []rune(name)
	// Classic two-pointer glob with backtracking to the last star.
	pi, ni, star, mark := 0, 0, -1, 0
	for ni < len(n) {
		switch {
		case pi < len(p) && p[pi] == '[':
			return false, refuse("scan with a character class", "KV lists by prefix only")
		case pi < len(p) && p[pi] == '*':
			star, mark = pi, ni
			pi++
		case pi < len(p) && p[pi] == '\\' && pi+1 < len(p) && p[pi+1] == n[ni]:
			pi += 2
			ni++
		case pi < len(p) && (p[pi] == '?' || (p[pi] != '\\' && p[pi] == n[ni])):
			pi++
			ni++
		case star >= 0:
			pi = star + 1
			mark++
			ni = mark
		default:
			return false, nil
		}
	}
	for pi < len(p) && p[pi] == '*' {
		pi++
	}
	return pi == len(p), nil
}

// expiry is a lease in whole seconds, rounded up, and never under [minTTL].
func expiry(ttl time.Duration) float64 {
	return math.Max(math.Ceil(ttl.Seconds()), minTTL.Seconds())
}

// escape makes a key safe in a URL path segment. The SDK puts the key into the
// path as given, so a key holding a slash or a question mark would otherwise
// address a different key, or none.
func escape(key string) string { return url.PathEscape(key) }

// missing reports whether a failure is KV saying the key is not there.
func missing(err error) bool {
	var apiErr *cloudflare.Error
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound
}

func refuse(op, why string) error {
	return fmt.Errorf("%w: %s cannot %s (%s)", cache.ErrUnsupported, backend, op, why)
}
