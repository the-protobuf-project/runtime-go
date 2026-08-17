package a2a

import (
	"context"
	"net"
	"sort"
	"testing"
	"time"

	"google.golang.org/grpc"
)

// waitFor polls until cond holds or the test gives up. Used for the one thing
// that has no callback to hang off: an OS listener becoming reachable.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition did not hold within 5s")
}

// reservePort binds a port, learns its number, and releases it — the usual way
// to get an address the test can predict without racing the whole suite for a
// fixed one.
func reservePort(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return addr
}

// keysOf renders a service map for a failure message.
func keysOf(info map[string]grpc.ServiceInfo) []string {
	out := make([]string, 0, len(info))
	for name := range info {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
