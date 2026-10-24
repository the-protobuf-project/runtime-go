package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/the-protobuf-project/runtime-go/grpc/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const slowMethod = "/test.Slow/Call"

// slowService is a one-method gRPC service whose handler parks until release
// is closed, counting each call into entered.
type slowService struct {
	entered atomic.Int32
	arrived chan struct{}
	release chan struct{}
}

func newSlowService() *slowService {
	return &slowService{arrived: make(chan struct{}, 1024), release: make(chan struct{})}
}

func (svc *slowService) register(s *grpc.Server) {
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Slow",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Call",
			Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				if err := dec(new(emptypb.Empty)); err != nil {
					return nil, err
				}
				svc.entered.Add(1)
				svc.arrived <- struct{}{}
				select {
				case <-svc.release:
					return new(emptypb.Empty), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}},
	}, svc)
}

// gateway stands in for a generated RegisterFooHandlerFromEndpoint: one
// client conn dialed from endpoint and opts, and GET /v1/slow proxying to
// slowMethod, answering a gRPC error with its HTTP status (Unavailable is the
// 503 a lost backend produces).
func (*slowService) gateway(mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) error {
	conn, err := grpc.NewClient(endpoint, opts...)
	if err != nil {
		return err
	}
	return mux.HandlePath(http.MethodGet, "/v1/slow", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		if err := conn.Invoke(r.Context(), slowMethod, new(emptypb.Empty), new(emptypb.Empty)); err != nil {
			http.Error(w, err.Error(), runtime.HTTPStatusFromCode(status.Code(err)))
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

// startSlowHybrid starts a HybridServer serving svc over gRPC and the REST
// gateway, and waits for the gateway to answer.
func startSlowHybrid(t *testing.T, svc *slowService, extra ...Option) (s *HybridServer, grpcAddr, httpAddr string) {
	t.Helper()
	ports := freePorts(t, 2)
	opts := baseOpts()
	opts.GRPC = options.GRPCOptions{Host: "127.0.0.1", Port: ports[0]}
	opts.HTTP = options.HTTPOptions{Host: "127.0.0.1", Port: ports[1]}
	opts.EnableHTTP = true

	s = NewHybridServer(opts, append([]Option{
		WithGRPCServers(svc.register),
		WithHTTPGateways(svc.gateway),
	}, extra...)...)
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop() })

	grpcAddr = fmt.Sprintf("127.0.0.1:%d", ports[0])
	httpAddr = fmt.Sprintf("127.0.0.1:%d", ports[1])
	resp := get(t, &http.Client{}, "http://"+httpAddr+"/health")
	if resp.status != http.StatusOK {
		t.Fatalf("GET /health = %d", resp.status)
	}
	return s, grpcAddr, httpAddr
}

type result struct {
	status int
	body   string
	err    error
}

func get(t *testing.T, client *http.Client, url string) result {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, http.NoBody)
	if err != nil {
		return result{err: err}
	}
	resp, err := client.Do(req)
	if err != nil {
		return result{err: err}
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	return result{status: resp.StatusCode, body: string(body), err: err}
}

// refused waits for a TCP dial to addr to fail, reporting whether it did
// before the deadline.
func refused(addr string) bool {
	dialer := net.Dialer{Timeout: 100 * time.Millisecond}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := dialer.DialContext(context.Background(), "tcp", addr)
		if err != nil {
			return true
		}
		_ = conn.Close()
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// TestStop_DrainsGatewayBeforeGRPC fires REST requests that park in the gRPC
// handler, calls Stop while they are in flight, and checks that new HTTP and
// gRPC connections are refused at once while every parked request still
// completes with 200 — the gateway's gRPC backend must outlive HTTP draining.
func TestStop_DrainsGatewayBeforeGRPC(t *testing.T) {
	const inFlight = 32

	svc := newSlowService()
	s, grpcAddr, httpAddr := startSlowHybrid(t, svc)

	// A connection per request, so each is in flight on its own.
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	results := make(chan result, inFlight)
	var wg sync.WaitGroup
	for range inFlight {
		wg.Go(func() { results <- get(t, client, "http://"+httpAddr+"/v1/slow") })
	}
	for range inFlight {
		select {
		case <-svc.arrived:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d requests reached the handler", svc.entered.Load(), inFlight)
		}
	}

	stopped := make(chan error, 1)
	go func() { stopped <- s.Stop() }()

	if !refused(httpAddr) {
		t.Error("HTTP listener still accepts connections after Stop began")
	}
	if !refused(grpcAddr) {
		t.Error("gRPC listener still accepts connections after Stop began")
	}
	select {
	case err := <-stopped:
		t.Fatalf("Stop returned (%v) before in-flight requests finished", err)
	default:
	}

	close(svc.release)
	wg.Wait()
	close(results)
	for r := range results {
		if r.err != nil || r.status != http.StatusOK {
			t.Errorf("in-flight request: status=%d err=%v body=%q", r.status, r.err, r.body)
		}
	}

	select {
	case err := <-stopped:
		if err != nil {
			t.Errorf("Stop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after in-flight requests finished")
	}
}

// TestStop_ShutdownTimeoutCutsOff checks that Stop gives up on requests still
// running when WithShutdownTimeout runs out, and says so.
func TestStop_ShutdownTimeoutCutsOff(t *testing.T) {
	svc := newSlowService()
	s, _, httpAddr := startSlowHybrid(t, svc, WithShutdownTimeout(200*time.Millisecond))
	defer close(svc.release)

	done := make(chan result, 1)
	go func() { done <- get(t, &http.Client{}, "http://"+httpAddr+"/v1/slow") }()
	select {
	case <-svc.arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach the handler")
	}

	start := time.Now()
	err := s.Stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Stop = %v, want a deadline error", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("Stop took %s with a 200ms timeout", elapsed)
	}
	if r := <-done; r.err == nil && r.status == http.StatusOK {
		t.Error("request cut off by the timeout still reported 200")
	}
}
