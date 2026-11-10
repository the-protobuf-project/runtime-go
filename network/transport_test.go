package network

import (
	"net/http"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// The pooled client must carry a dedicated transport — not the process-wide
// http.DefaultTransport, whose 2-idle-connections-per-host limit churns TCP
// connections under concurrent single-host traffic.
func TestNewPooledClientDedicatedTransport(t *testing.T) {
	c := newPooledClient(5*time.Second, telemetry.Signals{}, "http")
	if c.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v, want 5s", c.Timeout)
	}
	transport, ok := c.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport is %T, want *http.Transport", c.Transport)
	}
	if transport == http.DefaultTransport {
		t.Fatal("client shares http.DefaultTransport; want a dedicated transport")
	}
	if transport.MaxIdleConnsPerHost < 100 {
		t.Errorf("MaxIdleConnsPerHost = %d, want >= 100", transport.MaxIdleConnsPerHost)
	}
	if transport.MaxIdleConns < 100 {
		t.Errorf("MaxIdleConns = %d, want >= 100", transport.MaxIdleConns)
	}
}

// Each call must get its own transport: clients must not share pools.
func TestNewPooledClientIndependentTransports(t *testing.T) {
	a := newPooledClient(time.Second, telemetry.Signals{}, "http")
	b := newPooledClient(time.Second, telemetry.Signals{}, "http")
	if a.Transport == b.Transport {
		t.Fatal("two pooled clients share one transport; want independent pools")
	}
}
