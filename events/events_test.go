package events

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	want := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute}
	for i, w := range want {
		if got := Backoff(i + 1); got != w {
			t.Errorf("Backoff(%d) = %v, want %v", i+1, got, w)
		}
	}
	if got := Backoff(40); got != 6*time.Hour {
		t.Errorf("Backoff(40) = %v, want the 6h cap", got)
	}
}

func TestWants(t *testing.T) {
	created := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	c := &Change{Name: "sites/club/orders/r1", Type: "shop.order.confirmed", Time: created.Add(time.Hour), Scope: "sites/club"}
	w := func(types []string, scope string) *Webhook {
		return &Webhook{EventTypes: types, Scope: scope, CreateTime: created}
	}
	cases := []struct {
		hook *Webhook
		want bool
	}{
		{w(nil, ""), true},
		{w([]string{"shop.order.confirmed"}, ""), true},
		{w([]string{"shop.order.cancelled"}, ""), false},
		{w(nil, "sites/club"), true},
		{w(nil, "sites/clu"), false},
		{&Webhook{CreateTime: created.Add(2 * time.Hour)}, false},
	}
	for i, tc := range cases {
		if got := Wants(tc.hook, c); got != tc.want {
			t.Errorf("case %d: Wants = %v, want %v", i, got, tc.want)
		}
	}
	// A change of no scope goes only to webhooks of every scope.
	if Wants(w(nil, "sites/club"), &Change{Type: "x", Time: created}) {
		t.Error("an unscoped change went to a scoped webhook")
	}
}

func TestCloudEvent(t *testing.T) {
	id := EventID("sites/club/orders/r1", "e1")
	body, err := CloudEvent(id, "//shop.example.com", "shop.order.confirmed", "sites/club/orders/r1",
		time.Unix(1_800_000_000, 0), []byte(`{"state":"CONFIRMED"}`))
	if err != nil {
		t.Fatal(err)
	}
	var ev map[string]any
	if err := json.Unmarshal([]byte(body), &ev); err != nil {
		t.Fatal(err)
	}
	if ev["specversion"] != "1.0" || ev["id"] != id || ev["type"] != "shop.order.confirmed" ||
		ev["subject"] != "sites/club/orders/r1" || ev["source"] != "//shop.example.com" ||
		ev["time"] != "2027-01-15T08:00:00Z" || ev["datacontenttype"] != "application/json" {
		t.Fatalf("event %v", ev)
	}
	if data, _ := ev["data"].(map[string]any); data["state"] != "CONFIRMED" {
		t.Fatalf("data %v", ev["data"])
	}
	if EventID("sites/club/orders/r1", "e2") == id {
		t.Fatal("two versions of a record share an event id")
	}
	name := DeliveryName("webhooks/w", id)
	if !strings.HasPrefix(name, "webhooks/w/deliveries/d") || len(name) != len("webhooks/w/deliveries/d")+31 {
		t.Fatalf("DeliveryName = %q", name)
	}
	if DeliveryName("webhooks/w", id) != name || DeliveryName("webhooks/v", id) == name {
		t.Fatal("DeliveryName is not derived from (webhook, event)")
	}
	if WebhookOf(name) != "webhooks/w" || WebhookOf("webhooks/w") != "webhooks/w" {
		t.Fatalf("WebhookOf(%q) = %q", name, WebhookOf(name))
	}
}

func TestIsPublic(t *testing.T) {
	for addr, want := range map[string]bool{
		"8.8.8.8": true, "2606:4700::1111": true,
		"127.0.0.1": false, "::1": false, "10.1.2.3": false, "172.16.0.1": false, "192.168.1.1": false,
		"169.254.169.254": false, "fe80::1": false, "100.64.0.1": false, "0.0.0.0": false, "fd00::1": false,
	} {
		if got := IsPublic(net.ParseIP(addr)); got != want {
			t.Errorf("IsPublic(%s) = %v, want %v", addr, got, want)
		}
	}
	if err := PublicOnly("tcp", "127.0.0.1:443", nil); err == nil {
		t.Error("PublicOnly let loopback through")
	}
	if err := PublicOnly("tcp", "8.8.8.8:443", nil); err != nil {
		t.Errorf("PublicOnly refused a public address: %v", err)
	}
}

func TestCheckEndpoint(t *testing.T) {
	for _, tc := range []struct {
		uri    string
		policy EndpointPolicy
		reason string
	}{
		{"https://example.com/hooks", EndpointPolicy{}, ""},
		{"/hooks", EndpointPolicy{}, ReasonInvalidURI},
		{"https://u:p@example.com/hooks", EndpointPolicy{}, ReasonURIHasCredentials},
		{"http://example.com/hooks", EndpointPolicy{}, ReasonURINotHTTPS},
		{"http://localhost:8080/hooks", EndpointPolicy{}, ReasonURINotHTTPS},
		{"http://localhost:8080/hooks", EndpointPolicy{AllowInsecure: true}, ""},
		{"http://example.com/hooks", EndpointPolicy{AllowInsecure: true}, ReasonURINotHTTPS},
		{"https://10.0.0.1/hooks", EndpointPolicy{}, ReasonURINotPublic},
		{"https://10.0.0.1/hooks", EndpointPolicy{AllowPrivateNetworks: true}, ""},
		{"http://127.0.0.1/hooks", EndpointPolicy{AllowInsecure: true}, ReasonURINotPublic},
	} {
		err := CheckEndpoint(tc.uri, tc.policy)
		var e *EndpointError
		switch {
		case tc.reason == "" && err != nil:
			t.Errorf("CheckEndpoint(%q, %+v) = %v", tc.uri, tc.policy, err)
		case tc.reason != "" && (!errors.As(err, &e) || e.Reason != tc.reason):
			t.Errorf("CheckEndpoint(%q, %+v) = %v, want %s", tc.uri, tc.policy, err, tc.reason)
		}
	}
}

func TestLifecycle(t *testing.T) {
	at := func(sec int) time.Time { return time.Date(2026, 11, 2, 10, 0, sec, 0, time.UTC) }
	for _, c := range []struct {
		create, update, del time.Time
		want                string
	}{
		{at(0), at(0), time.Time{}, Created},
		{at(0), at(5), time.Time{}, Updated},
		{at(0), at(9), at(9), Deleted},
		// Deleted by code that wrote the two a moment apart.
		{at(0), at(9), at(8), Deleted},
		// Its expire_time stamped long after: not a deletion to send again.
		{at(0), at(40), at(9), ""},
	} {
		if got := Lifecycle(c.create, c.update, c.del); got != c.want {
			t.Errorf("Lifecycle(%v, %v, %v) = %q, want %q", c.create, c.update, c.del, got, c.want)
		}
	}
}

func TestSignAndVerify(t *testing.T) {
	secret, err := NewSecret()
	if err != nil || !strings.HasPrefix(secret, SecretPrefix) {
		t.Fatalf("NewSecret = %q, %v", secret, err)
	}
	// The Standard Webhooks specification's test vector.
	const vecSecret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	const vecID, vecTS, vecBody = "msg_p5jXN8AQM9LWM0D4loKWxJek", "1614265330", `{"test": 2432232314}`
	const vecSig = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
	got, err := Sign(vecSecret, vecID, time.Unix(1614265330, 0), []byte(vecBody))
	if err != nil || got != vecSig {
		t.Fatalf("Sign = %q, %v; want %q", got, err, vecSig)
	}
	if !Verify(vecSecret, vecID, vecTS, []byte(vecBody), "v1,bogus "+vecSig) {
		t.Fatal("Verify rejected a valid signature among several")
	}
	if Verify(vecSecret, vecID, vecTS, []byte(vecBody+" "), vecSig) || Verify(secret, vecID, vecTS, []byte(vecBody), vecSig) {
		t.Fatal("Verify accepted a wrong body or key")
	}
	if _, err := Sign("whsec_!!", "id", time.Now(), nil); !errors.Is(err, ErrUnreadableSecret) {
		t.Fatalf("an unreadable secret signed: %v", err)
	}
}

func key(b byte) string { return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, 32)) }

func TestSealer(t *testing.T) {
	const secret = "whsec_c2VjcmV0"
	old, err := NewSealer(key(1), nil)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := old.Seal(secret)
	if err != nil || !strings.HasPrefix(sealed, SealedPrefix) || strings.Contains(sealed, "c2VjcmV0") {
		t.Fatalf("Seal = %q, %v; want a sealed value hiding the secret", sealed, err)
	}
	if again, _ := old.Seal(secret); again == sealed {
		t.Error("two seals of one secret are identical; the nonce is not random")
	}

	// Rotated key: what the old key sealed still opens; new seals use the new key.
	s, err := NewSealer(key(2), []string{key(1)})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.Open(sealed); err != nil || got != secret {
		t.Fatalf("Open under a previous key = %q, %v", got, err)
	}
	resealed, _ := s.Seal(secret)
	if _, err := old.Open(resealed); err == nil {
		t.Error("a sealer without the new key opened what it sealed")
	}

	// A secret stored before sealing is used as it is.
	if got, err := s.Open(secret); err != nil || got != secret {
		t.Errorf("Open(unsealed) = %q, %v", got, err)
	}
	// Without any key, a sealed value cannot be used, and says why.
	var none *Sealer
	if _, err := none.Open(sealed); err == nil || !strings.Contains(err.Error(), "secret key") {
		t.Errorf("Open without a key: %v", err)
	}
	if got, _ := none.Seal(secret); got != secret {
		t.Errorf("a nil Sealer changed the secret: %q", got)
	}
	// Tampering is caught, not signed with garbage.
	tampered := sealed[:len(sealed)-4] + "AAA="
	if _, err := s.Open(tampered); err == nil {
		t.Error("a tampered secret opened")
	}
	if none, err := NewSealer("", nil); none != nil || err != nil {
		t.Errorf("NewSealer with no key = %v, %v; want no sealer", none, err)
	}
	for _, bad := range [][2]string{{"short", ""}, {"", key(1)}} {
		var prev []string
		if bad[1] != "" {
			prev = []string{bad[1]}
		}
		if _, err := NewSealer(bad[0], prev); err == nil {
			t.Errorf("NewSealer(%q, %v) accepted", bad[0], prev)
		}
	}
}

// TestRecordAllFetchesWhatIsSent: a page is read as far as the read mask,
// whole records are fetched once, for the changes sent, and a record that
// moved on since the page was read is left to the read of its new version.
func TestRecordAllFetchesWhatIsSent(t *testing.T) {
	at := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	full := map[string]fetched{
		"sites/club/orders/r1": {etag: "e1", data: func() ([]byte, error) { return []byte(`{"customer":"users/ann"}`), nil }},
		"sites/club/orders/r2": {etag: "e2-newer", data: func() ([]byte, error) { return []byte(`{}`), nil }},
	}
	var fetchedNames [][]string
	f := Feed{id: "orders", fetch: func(_ context.Context, names []string) (map[string]fetched, error) {
		fetchedNames = append(fetchedNames, names)
		return full, nil
	}}
	st := newMemStore()
	d := NewDispatcher(st, nil, Options{Source: "//test", Now: func() time.Time { return at }})
	hooks := []Webhook{{Name: "webhooks/w", CreateTime: at.Add(-time.Hour)}}
	changes := []Change{
		{Name: "sites/club/orders/r1", Etag: "e1", Type: "shop.order.confirmed", Time: at},
		{Name: "sites/club/orders/r2", Etag: "e2", Type: "shop.order.confirmed", Time: at},
		{Name: "sites/club/orders/r3", Etag: "e3", Type: "shop.order.confirmed", Time: at},
	}
	if err := d.recordAll(context.Background(), f, changes, hooks); err != nil {
		t.Fatal(err)
	}
	if len(fetchedNames) != 1 || !slices.Equal(fetchedNames[0], []string{changes[0].Name, changes[1].Name, changes[2].Name}) {
		t.Fatalf("fetched %v, want the three names in one read", fetchedNames)
	}
	created := st.all()
	if len(created) != 1 || created[0].EventID != EventID(changes[0].Name, changes[0].Etag) {
		t.Fatalf("created %v, want only r1's delivery under its change's event id", created)
	}
	if !d.seen(&changes[0]) || d.seen(&changes[1]) || d.seen(&changes[2]) {
		t.Fatal("only the recorded change is seen; a superseded or purged version is not")
	}
	if err := d.recordAll(context.Background(), f, nil, hooks); err != nil || len(fetchedNames) != 1 {
		t.Fatalf("an empty page fetched (%d reads, %v)", len(fetchedNames), err)
	}
}
