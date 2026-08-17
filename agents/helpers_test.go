package agents

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// fake is a Service that records where it was placed, so a test can assert on
// the runtime's decisions rather than on a protocol's behavior.
type fake struct {
	protocol Protocol
	requires Requirements
	path     string // mounted on the placement's mux, when it wants HTTP

	failBefore bool          // return an error without ever reporting ready
	stall      bool          // never report ready at all
	readyAfter time.Duration // delay before reporting ready

	placement chan Placement
}

func newFake(p Protocol, req Requirements, path string) *fake {
	return &fake{protocol: p, requires: req, path: path, placement: make(chan Placement, 1)}
}

func (f *fake) Protocol() Protocol     { return f.protocol }
func (f *fake) Requires() Requirements { return f.requires }
func (f *fake) placed() Placement      { return <-f.placement }
func (f *fake) Serve(ctx context.Context, p Placement, ready func([]Endpoint)) error {
	if f.failBefore {
		return errors.New("refused to start")
	}
	if f.requires.HTTP && f.path != "" {
		p.Mux.HandleFunc(f.path, func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprintf(w, "%s here", f.protocol)
		})
	}
	f.placement <- p

	if f.stall {
		<-ctx.Done()
		return nil
	}
	if f.readyAfter > 0 {
		time.Sleep(f.readyAfter)
	}
	ready([]Endpoint{{
		Protocol:  f.protocol,
		Transport: "test",
		URL:       "http://" + p.Addr + f.path,
	}})

	<-ctx.Done()
	return nil
}

func freePort(t *testing.T) int {
	t.Helper()
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	if err := lis.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return port
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(body)
}
