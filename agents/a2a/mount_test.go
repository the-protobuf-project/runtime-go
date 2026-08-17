package a2a

import (
	"context"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc"
)

// With a shared mux the host owns the listener, so StartServer must return on
// cancellation without trying to shut anything down.
func TestStartServer_SharedMuxReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- StartServer(ctx, &ServerConfig{Name: "test", Mux: http.NewServeMux()}, echoAgent())
	}()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("got %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("StartServer did not return after cancellation")
	}
}

// Owning the listener means draining it: canceling must stop the server rather
// than leave it bound, and must not surface ErrServerClosed as a failure.
func TestStartServer_OwnedListenerShutsDownOnCancel(t *testing.T) {
	addr := reservePort(t)
	cfg := &ServerConfig{Name: "echo", Addr: addr}

	ready := make(chan struct{})
	cfg.OnReady = func(*ServerConfig) { close(ready) }

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- StartServer(ctx, cfg, echoAgent()) }()

	<-ready
	waitFor(t, func() bool {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
			"http://"+addr+AgentCardPath, nil)
		if err != nil {
			return false
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("got %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("StartServer did not return after cancellation")
	}
}

// gRPC-only serves nothing of its own — it registers on the caller's server and
// then just tracks the caller's lifetime.
func TestStartServer_GRPCOnlyRegistersAndBlocks(t *testing.T) {
	grpcSrv := grpc.NewServer()
	cfg := &ServerConfig{Name: "echo", Transport: TransportGRPC, GRPCServer: grpcSrv}

	ready := make(chan struct{})
	cfg.OnReady = func(*ServerConfig) { close(ready) }

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- StartServer(ctx, cfg, echoAgent()) }()

	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("OnReady was never called")
	}

	if _, ok := grpcSrv.GetServiceInfo()[GRPCServiceName]; !ok {
		t.Errorf("A2A service was not registered; got %v", keysOf(grpcSrv.GetServiceInfo()))
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("got %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("StartServer did not return after cancellation")
	}
}
