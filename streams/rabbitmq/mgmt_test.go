package rabbitmq_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sync"
	"testing"
	"time"
)

// connections maps each test to the connection names its stores gave the
// broker, so one test drops its own connections and nobody else's.
var connections sync.Map // *testing.T -> []string

// remember records that t opened a connection named name.
func remember(t *testing.T, name string) {
	mine, _ := connections.Load(t)
	names, _ := mine.([]string)
	connections.Store(t, append(names, name))
	t.Cleanup(func() { connections.Delete(t) })
}

// dropConnections closes the connections this test opened from the broker's
// side, which is what a network failure looks like to the client. It goes
// through the management API, and skips the test when that is not there.
func dropConnections(t *testing.T) {
	t.Helper()

	mine, _ := connections.Load(t)
	want, _ := mine.([]string)
	if len(want) == 0 {
		t.Fatal("dropConnections: this test opened no connection")
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		// The management API lists a connection a moment after it opens.
		if names := connectionsNamed(t, want); len(names) > 0 {
			for _, name := range names {
				mgmt(t, http.MethodDelete, "/api/connections/"+url.PathEscape(name), nil)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("this test's connection never appeared in the management API")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// connectionsNamed names the open connections whose connection_name is one of
// want.
func connectionsNamed(t *testing.T, want []string) []string {
	t.Helper()

	var conns []struct {
		Name             string `json:"name"`
		ClientProperties struct {
			ConnectionName string `json:"connection_name"`
		} `json:"client_properties"`
	}
	mgmt(t, http.MethodGet, "/api/connections", &conns)

	var names []string
	for _, c := range conns {
		if slices.Contains(want, c.ClientProperties.ConnectionName) {
			names = append(names, c.Name)
		}
	}
	return names
}

// mgmt calls the management API, decoding the response into into when it is
// not nil. It fails on any response other than a success.
func mgmt(t *testing.T, method, path string, into any) {
	t.Helper()

	base := os.Getenv("RABBITMQ_TEST_MANAGEMENT")
	if base == "" {
		base = "http://127.0.0.1:15672"
	}
	req, err := http.NewRequestWithContext(t.Context(), method, base+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("guest", "guest")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Skipf("no RabbitMQ management API at %s (%v); skipping", base, err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode < 200 || res.StatusCode > 299 {
		t.Fatalf("management API %s %s: %s", method, path, res.Status)
	}
	if into != nil {
		if err := json.NewDecoder(res.Body).Decode(into); err != nil {
			t.Fatalf("management API %s %s: %v", method, path, err)
		}
	}
}
