package grpc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/the-protobuf-project/runtime-go/grpc/shared"
	"google.golang.org/grpc"
)

// Start validates the server configuration and launches the gRPC server and any
// enabled gateways (HTTP/1.1, experimental HTTP/3). Each server component runs
// in its own goroutine. Once all components are up a startup summary table is
// printed to stdout.
func (s *HybridServer) Start() error {
	shared.Telemetry().Logger.Debugf("Start: validating server options")
	if err := s.validateOptions(); err != nil {
		return err
	}
	shared.Telemetry().Logger.Debugf("Start: options valid — gRPC=%s:%d enableHTTP=%v enableMCP=%v",
		s.opts.GRPC.Host, s.opts.GRPC.Port,
		s.opts.EnableHTTP, s.opts.EnableMCP)

	shared.Telemetry().Logger.Debugf("Start: starting gRPC server")
	if err := s.startGRPCServer(); err != nil {
		return err
	}

	if s.opts.ExperimentalHttp3 {
		shared.Telemetry().Logger.Debugf("Start: starting experimental HTTP/3 server")
		s.startHTTP3ExperimentalServer()
	}

	if s.opts.EnableHTTP {
		shared.Telemetry().Logger.Debugf("Start: starting HTTP/1.1 gateway")
		if err := s.startHTTPGateway(); err != nil {
			return err
		}
	} else {
		shared.Telemetry().Logger.Debugf("Start: HTTP/1.1 gateway disabled (EnableHTTP=false)")
	}

	s.printStartupBanner(s.agentEndpoints())
	return nil
}

func (s *HybridServer) Close() error {
	shared.Telemetry().Logger.Debugf("Close: initiating graceful close")
	go func() {
		_ = shared.Close()
	}()
	return s.Stop()
}

// Stop gracefully shuts down all running servers, allowing in-flight requests
// to complete before closing connections. The whole sequence is bounded by
// [WithShutdownTimeout]:
//
//  1. Stop accepting: the gRPC TCP listener closes, and the HTTP/1.1, HTTP/3
//     and agent servers close theirs as their shutdown begins.
//  2. Drain HTTP: in-flight gateway, HTTP/3 and agent requests finish while
//     the gRPC server keeps serving them over the in-memory gateway listener.
//  3. Drain gRPC: GracefulStop waits for the remaining RPCs, then closes the
//     gateway listener and every connection.
//
// Whatever is still running when the timeout runs out is cut off, and Stop
// reports it.
func (s *HybridServer) Stop() error {
	shared.Telemetry().Logger.Info("Shutting down servers...")
	ctx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()

	s.draining.Store(true)
	if s.grpcListener != nil {
		shared.Telemetry().Logger.Debugf("Stop: closing the gRPC TCP listener")
		// Serve may already have closed it; the error says nothing new.
		_ = s.grpcListener.Close()
		s.grpcListener = nil
	}

	errs := s.drainHTTP(ctx)

	if s.grpcServer != nil {
		shared.Telemetry().Logger.Debugf("Stop: calling GracefulStop on gRPC server")
		if err := stopGRPC(ctx, s.grpcServer); err != nil {
			errs = append(errs, err)
		}
		shared.Telemetry().Logger.Debugf("Stop: gRPC server stopped")
	}
	s.gatewayListener = nil
	return errors.Join(errs...)
}

// drainHTTP shuts down the HTTP/1.1 and HTTP/3 servers and the agent
// protocols together, each closing its listeners and then waiting for its
// in-flight requests. A server still busy when ctx ends is closed outright.
func (s *HybridServer) drainHTTP(ctx context.Context) []error {
	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
	)
	fail := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err)
	}

	if srv := s.httpServer; srv != nil {
		wg.Go(func() {
			shared.Telemetry().Logger.Debugf("Stop: shutting down HTTP/1.1 server")
			if err := srv.Shutdown(ctx); err != nil {
				_ = srv.Close()
				fail(fmt.Errorf("failed to shutdown HTTP server: %w", err))
				return
			}
			shared.Telemetry().Logger.Debugf("Stop: HTTP/1.1 server stopped")
		})
	}
	if srv := s.http3Server; srv != nil && s.opts.ExperimentalHttp3 {
		wg.Go(func() {
			shared.Telemetry().Logger.Debugf("Stop: shutting down HTTP/3 server")
			if err := srv.Shutdown(ctx); err != nil {
				_ = srv.Close()
				fail(fmt.Errorf("failed to shutdown HTTP/3 server: %w", err))
			}
		})
	}
	wg.Go(func() {
		if err := s.stopAgents(ctx); err != nil {
			fail(fmt.Errorf("failed to shutdown agent protocols: %w", err))
		}
	})

	wg.Wait()
	return errs
}

// stopGRPC runs GracefulStop, falling back to Stop if ctx ends first.
func stopGRPC(ctx context.Context, srv *grpc.Server) error {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		srv.Stop()
		<-done
		return fmt.Errorf("failed to drain gRPC server: %w", ctx.Err())
	}
}

// Restart gracefully stops and then starts the server again. This is useful
// for applying configuration reloads or performing hot restarts without killing
// the main process.
func (s *HybridServer) Restart() error {
	shared.Telemetry().Logger.Info("Restarting servers...")
	shared.Telemetry().Logger.Debugf("Restart: stopping all components before restart")
	if err := s.Stop(); err != nil {
		return fmt.Errorf("failed to stop servers during restart: %w", err)
	}
	shared.Telemetry().Logger.Debugf("Restart: all components stopped, starting again")
	return s.Start()
}
