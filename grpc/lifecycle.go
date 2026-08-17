package grpc

import (
	"context"
	"fmt"

	"github.com/the-protobuf-project/runtime-go/grpc/shared"
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
// to complete before closing connections.
func (s *HybridServer) Stop() error {
	shared.Telemetry().Logger.Info("Shutting down servers...")
	if s.grpcServer != nil {
		shared.Telemetry().Logger.Debugf("Stop: calling GracefulStop on gRPC server")
		s.grpcServer.GracefulStop()
		shared.Telemetry().Logger.Debugf("Stop: gRPC server stopped")
	}
	if s.httpServer != nil {
		shared.Telemetry().Logger.Debugf("Stop: shutting down HTTP/1.1 server")
		if err := s.httpServer.Shutdown(context.Background()); err != nil {
			return fmt.Errorf("failed to shutdown HTTP server: %w", err)
		}
		shared.Telemetry().Logger.Debugf("Stop: HTTP/1.1 server stopped")
	}
	if err := s.stopAgents(); err != nil {
		return fmt.Errorf("failed to shutdown agent protocols: %w", err)
	}
	return nil
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
