package workerskv_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudflare/cloudflare-go/v7"
	"github.com/cloudflare/cloudflare-go/v7/option"

	"github.com/the-protobuf-project/runtime-go/cache"
	"github.com/the-protobuf-project/runtime-go/cache/workerskv"
)

const (
	account   = "acct"
	namespace = "ns1"
)

// fakeKV serves the slice of the Workers KV REST API the driver uses.
type fakeKV struct {
	mu      sync.Mutex
	values  map[string][]byte
	expires map[string]int64 // unix seconds; absent never expires
	ttls    []float64        // every expiration_ttl a write asked for
}

func newFake(t *testing.T) (*fakeKV, *cloudflare.Client) {
	t.Helper()
	f := &fakeKV{values: map[string][]byte{}, expires: map[string]int64{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, cloudflare.NewClient(option.WithBaseURL(srv.URL+"/"), option.WithAPIToken("test"), option.WithMaxRetries(0))
}

func (f *fakeKV) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	base := "/accounts/" + account + "/storage/kv/namespaces/" + namespace
	raw := r.URL.EscapedPath()
	if !strings.HasPrefix(raw, base) {
		fail(w, http.StatusNotFound, 10013, "namespace not found")
		return
	}
	rest := strings.TrimPrefix(raw, base)
	switch {
	case rest == "" && r.Method == http.MethodGet:
		reply(w, map[string]any{"id": namespace, "title": "test"})
	case strings.HasPrefix(rest, "/values/"):
		key, err := url.PathUnescape(strings.TrimPrefix(rest, "/values/"))
		if err != nil {
			fail(w, http.StatusBadRequest, 10000, err.Error())
			return
		}
		f.value(w, r, key)
	case rest == "/bulk/get" && r.Method == http.MethodPost:
		var body struct {
			Keys []string `json:"keys"`
			Type string   `json:"type"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if len(body.Keys) > 100 || body.Type != "text" {
			fail(w, http.StatusBadRequest, 10000, "bad bulk get")
			return
		}
		values := map[string]any{}
		for _, k := range body.Keys {
			if v, ok := f.live(k); ok {
				values[k] = string(v)
			} else {
				values[k] = nil
			}
		}
		reply(w, map[string]any{"values": values})
	case rest == "/bulk/delete" && r.Method == http.MethodPost:
		var keys []string
		_ = json.NewDecoder(r.Body).Decode(&keys)
		for _, k := range keys {
			delete(f.values, k)
			delete(f.expires, k)
		}
		reply(w, map[string]any{"successful_key_count": len(keys), "unsuccessful_keys": []string{}})
	case rest == "/keys" && r.Method == http.MethodGet:
		f.list(w, r)
	default:
		fail(w, http.StatusNotFound, 10000, "no route for "+r.Method+" "+raw)
	}
}

func (f *fakeKV) live(key string) ([]byte, bool) {
	v, ok := f.values[key]
	if !ok {
		return nil, false
	}
	if exp, has := f.expires[key]; has && time.Now().Unix() >= exp {
		return nil, false
	}
	return v, true
}

func (f *fakeKV) value(w http.ResponseWriter, r *http.Request, key string) {
	switch r.Method {
	case http.MethodGet:
		v, ok := f.live(key)
		if !ok {
			fail(w, http.StatusNotFound, 10009, "get: 'key not found'")
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(v)
	case http.MethodPut:
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			fail(w, http.StatusBadRequest, 10000, err.Error())
			return
		}
		delete(f.expires, key)
		if s := r.URL.Query().Get("expiration_ttl"); s != "" {
			ttl, _ := strconv.ParseFloat(s, 64)
			if ttl < 60 {
				fail(w, http.StatusBadRequest, 10019, "Invalid expiration_ttl of "+s+". Expiration TTL must be at least 60.")
				return
			}
			f.ttls = append(f.ttls, ttl)
			f.expires[key] = time.Now().Unix() + int64(ttl)
		}
		f.values[key] = []byte(r.MultipartForm.Value["value"][0])
		reply(w, map[string]any{})
	default:
		fail(w, http.StatusMethodNotAllowed, 10000, "method")
	}
}

// list pages by cursor, two keys a page, so paging is exercised.
func (f *fakeKV) list(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	var names []string
	for k := range f.values {
		if _, ok := f.live(k); ok && strings.HasPrefix(k, prefix) {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	start, _ := strconv.Atoi(r.URL.Query().Get("cursor"))
	end := min(start+2, len(names))
	result := make([]map[string]any, 0, end-start)
	for _, k := range names[start:end] {
		item := map[string]any{"name": k}
		if exp, ok := f.expires[k]; ok {
			item["expiration"] = exp
		}
		result = append(result, item)
	}
	cursor := ""
	if end < len(names) {
		cursor = strconv.Itoa(end)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": true, "errors": []any{}, "messages": []any{}, "result": result,
		"result_info": map[string]any{"count": len(result), "cursor": cursor},
	})
}

func reply(w http.ResponseWriter, result any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "errors": []any{}, "messages": []any{}, "result": result})
}

func fail(w http.ResponseWriter, status, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": false, "result": nil, "messages": []any{},
		"errors": []map[string]any{{"code": code, "message": msg}},
	})
}

func open(t *testing.T, cfg cache.Config) (*fakeKV, *cache.DB) {
	t.Helper()
	f, cf := newFake(t)
	client, err := workerskv.NewClient(t.Context(), workerskv.Config{Client: cf, AccountID: account, NamespaceID: namespace})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	db, err := workerskv.New(client, cfg).SetDatabase(t.Context(), "orders")
	if err != nil {
		t.Fatalf("SetDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return f, db
}

type session struct {
	User string `json:"user"`
}

func TestVolatileRoundTripWithAwkwardKeys(t *testing.T) {
	_, db := open(t, cache.Config{Prefix: "app"})
	ctx := t.Context()
	for _, key := range []string{"plain", "with/slash", "with?query", "100%", "spa ce"} {
		if err := db.Volatile.Set(ctx, key, session{User: key}, cache.TTL(time.Hour)); err != nil {
			t.Fatalf("Set %q: %v", key, err)
		}
		var got session
		if err := db.Volatile.Get(ctx, key, &got); err != nil || got.User != key {
			t.Fatalf("Get %q: %v, %+v", key, err, got)
		}
	}
	if err := db.Volatile.Delete(ctx, "with/slash"); err != nil {
		t.Fatal(err)
	}
	var got session
	if err := db.Volatile.Get(ctx, "with/slash", &got); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("after delete: %v", err)
	}
}

func TestShortLeasesAreLengthenedToTheMinimum(t *testing.T) {
	f, db := open(t, cache.Config{})
	ctx := t.Context()
	if err := db.Volatile.Set(ctx, "k", 1, cache.TTL(5*time.Second)); err != nil {
		t.Fatalf("a short lease was refused: %v", err)
	}
	if err := db.Volatile.Set(ctx, "k2", 1, cache.TTL(90500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if want := []float64{60, 91}; len(f.ttls) != 2 || f.ttls[0] != want[0] || f.ttls[1] != want[1] {
		t.Fatalf("ttls sent: %v, want %v", f.ttls, want)
	}
	left, err := db.Volatile.TTL(ctx, "k2")
	if err != nil || left <= 80*time.Second || left > 91*time.Second {
		t.Fatalf("TTL = %v, %v", left, err)
	}
	if err := db.Volatile.Set(ctx, "forever", 1, cache.NoExpiry()); err != nil {
		t.Fatal(err)
	}
	if left, err := db.Volatile.TTL(ctx, "forever"); err != nil || left != 0 {
		t.Fatalf("TTL of a permanent key = %v, %v", left, err)
	}
	if _, err := db.Volatile.TTL(ctx, "absent"); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("TTL of an absent key: %v", err)
	}
}

func TestConditionalWritesRefuse(t *testing.T) {
	_, db := open(t, cache.Config{DefaultTTL: time.Hour})
	ctx := t.Context()
	id, err := db.Document.Create(ctx, session{User: "a"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := db.Document.Update(ctx, id, session{User: "b"}); !errors.Is(err, cache.ErrUnsupported) {
		t.Fatalf("Update: got %v, want ErrUnsupported", err)
	}
	if err := db.Volatile.Touch(ctx, "k", time.Hour); !errors.Is(err, cache.ErrUnsupported) {
		t.Fatalf("Touch: got %v, want ErrUnsupported", err)
	}
	var got session
	if err := db.Document.Get(ctx, id, &got); err != nil || got.User != "a" {
		t.Fatalf("Get after a refused update: %v, %+v", err, got)
	}
}

func TestReadThroughLoadsOnce(t *testing.T) {
	_, db := open(t, cache.Config{DefaultTTL: time.Hour})
	ctx := t.Context()
	loads := 0
	aside := db.Aside(func(_ context.Context, id string) (any, error) {
		loads++
		return session{User: id}, nil
	})
	for range 3 {
		var got session
		if err := aside.GetOrLoad(ctx, "u1", &got); err != nil || got.User != "u1" {
			t.Fatalf("GetOrLoad: %v, %+v", err, got)
		}
	}
	if loads != 1 {
		t.Fatalf("loaded %d times", loads)
	}
}

func TestScanPagesAndMatchesGlobs(t *testing.T) {
	_, db := open(t, cache.Config{Prefix: "app"})
	ctx := t.Context()
	for _, key := range []string{"user:1", "user:2", "user:10", "order:1", "user-x"} {
		if err := db.Volatile.Set(ctx, key, 1, cache.TTL(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := db.Volatile.Scan(ctx, "user:?")
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(keys) != 2 || !strings.HasSuffix(keys[0], "user:1") || !strings.HasSuffix(keys[1], "user:2") {
		t.Fatalf("Scan user:? = %v", keys)
	}
	all, err := db.Volatile.Scan(ctx, "*")
	if err != nil || len(all) != 5 {
		t.Fatalf("Scan * = %v, %v", all, err)
	}
	if _, err := db.Volatile.Scan(ctx, "user:[12]"); !errors.Is(err, cache.ErrUnsupported) {
		t.Fatalf("character class: %v", err)
	}
}

func TestBulkReadsKeepOrderAcrossPieces(t *testing.T) {
	f, cf := newFake(t)
	client, err := workerskv.NewClient(t.Context(), workerskv.Config{Client: cf, AccountID: account, NamespaceID: namespace})
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, 250)
	for i := range 250 { // three requests at a hundred keys each
		key := "k" + strconv.Itoa(i)
		keys = append(keys, key)
		if i%2 == 0 {
			f.values[key] = []byte(`"` + key + `"`)
		}
	}
	got, err := workerskv.GetMany(t.Context(), client, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	for i, key := range keys {
		want := ""
		if i%2 == 0 {
			want = `"` + key + `"`
		}
		if string(got[i]) != want || (want == "") != (got[i] == nil) {
			t.Fatalf("GetMany[%d] = %q, want %q", i, got[i], want)
		}
	}
}

func TestDropDatabase(t *testing.T) {
	f, cf := newFake(t)
	client, err := workerskv.NewClient(t.Context(), workerskv.Config{Client: cf, AccountID: account, NamespaceID: namespace})
	if err != nil {
		t.Fatal(err)
	}
	p := workerskv.New(client, cache.Config{Prefix: "app"})
	ctx := t.Context()
	orders, _ := p.SetDatabase(ctx, "orders")
	users, _ := p.SetDatabase(ctx, "users")
	for _, k := range []string{"a", "b", "c"} {
		_ = orders.Volatile.Set(ctx, k, 1, cache.TTL(time.Hour))
	}
	_ = users.Volatile.Set(ctx, "a", 1, cache.TTL(time.Hour))

	n, err := p.DropDatabase(ctx, "orders")
	if err != nil || n != 3 {
		t.Fatalf("DropDatabase = %d, %v", n, err)
	}
	if len(f.values) != 1 {
		t.Fatalf("left behind %d keys, want users' one", len(f.values))
	}
}

func TestNewClientChecksTheNamespace(t *testing.T) {
	_, cf := newFake(t)
	if _, err := workerskv.NewClient(t.Context(), workerskv.Config{Client: cf, AccountID: account, NamespaceID: "nope"}); err == nil {
		t.Fatal("an unknown namespace was accepted")
	}
	if _, err := workerskv.NewClient(t.Context(), workerskv.Config{AccountID: account, NamespaceID: namespace}); err == nil {
		t.Fatal("a missing client was accepted")
	}
}
