package grpc

import (
	"crypto/tls"
	"fmt"
	"os"

	"github.com/the-protobuf-project/runtime-go/grpc/shared"
)

const (
	// defaultHTTPPort is the default port for the HTTP gateway if not specified.
	defaultHTTPPort = 6502
	// defaultGRPCPort is the default port for the gRPC server if not specified.
	defaultGRPCPort = 65020
)

// validateOptions performs pre-flight checks on the HybridServer's configuration.
// It ensures required fields like hosts are set, assigns default ports if they
// are not specified, and validates that all port numbers are within the valid
// TCP range.
func (s *HybridServer) validateOptions() error {
	shared.Telemetry().Logger.Debugf("validateOptions: checking gRPC host=%q port=%d", s.opts.GRPC.Host, s.opts.GRPC.Port)
	if s.opts.GRPC.Host == "" {
		return fmt.Errorf("GRPC.Host must not be empty")
	}
	if s.opts.GRPC.Port == 0 {
		shared.Telemetry().Logger.Debugf("validateOptions: gRPC port not set, applying default %d", defaultGRPCPort)
		s.opts.GRPC.Port = defaultGRPCPort
	}
	if s.opts.GRPC.Port < 1 || s.opts.GRPC.Port > 65535 {
		return fmt.Errorf("GRPC.Port must be between 1 and 65535")
	}
	shared.Telemetry().Logger.Debugf("validateOptions: gRPC %s:%d OK", s.opts.GRPC.Host, s.opts.GRPC.Port)

	if s.opts.EnableHTTP {
		shared.Telemetry().Logger.Debugf("validateOptions: checking HTTP host=%q port=%d", s.opts.HTTP.Host, s.opts.HTTP.Port)
		if s.opts.HTTP.Host == "" {
			return fmt.Errorf("HTTP.Host must not be empty if HTTP is enabled")
		}
		if s.opts.HTTP.Port == 0 {
			shared.Telemetry().Logger.Debugf("validateOptions: HTTP port not set, applying default %d", defaultHTTPPort)
			s.opts.HTTP.Port = defaultHTTPPort
		}
		if s.opts.HTTP.Port < 1 || s.opts.HTTP.Port > 65535 {
			return fmt.Errorf("HTTP.Port must be between 1 and 65535 if HTTP is enabled")
		}
		shared.Telemetry().Logger.Debugf("validateOptions: HTTP %s:%d OK", s.opts.HTTP.Host, s.opts.HTTP.Port)
	} else {
		shared.Telemetry().Logger.Debugf("validateOptions: HTTP gateway disabled")
	}

	if s.opts.EnableMCP && s.opts.MCP.Transport != "" && s.opts.MCP.Transport != "stdio" {
		shared.Telemetry().Logger.Debugf("validateOptions: checking MCP host=%q port=%d transport=%q",
			s.opts.MCP.Host, s.opts.MCP.Port, s.opts.MCP.Transport)
		if s.opts.MCP.Host == "" {
			shared.Telemetry().Logger.Debugf("validateOptions: MCP host not set, defaulting to 0.0.0.0")
			s.opts.MCP.Host = "0.0.0.0"
		}
		if s.opts.MCP.Port == 0 {
			s.opts.MCP.Port = s.opts.HTTP.Port + 1
			shared.Telemetry().Logger.Debugf("validateOptions: MCP port not set, defaulting to HTTP+1=%d", s.opts.MCP.Port)
		}
		if s.opts.MCP.Port < 1 || s.opts.MCP.Port > 65535 {
			return fmt.Errorf("MCP.Port must be between 1 and 65535")
		}
		shared.Telemetry().Logger.Debugf("validateOptions: MCP %s:%d transport=%s OK",
			s.opts.MCP.Host, s.opts.MCP.Port, s.opts.MCP.Transport)
	} else if s.opts.EnableMCP {
		shared.Telemetry().Logger.Debugf("validateOptions: MCP enabled with transport=%q (stdio or unset — no port binding)",
			s.opts.MCP.Transport)
	}

	if s.opts.EnableA2A {
		shared.Telemetry().Logger.Debugf("validateOptions: checking A2A host=%q port=%d transport=%q",
			s.opts.A2A.Host, s.opts.A2A.Port, s.opts.A2A.Transport)
		if s.opts.A2A.Host == "" {
			shared.Telemetry().Logger.Debugf("validateOptions: A2A host not set, defaulting to 0.0.0.0")
			s.opts.A2A.Host = "0.0.0.0"
		}
		if s.opts.A2A.Port == 0 {
			// One past MCP, which is itself one past HTTP — so the four layers
			// land on consecutive ports without the caller assigning any.
			base := s.opts.MCP.Port
			if base == 0 {
				base = s.opts.HTTP.Port + 1
			}
			s.opts.A2A.Port = base + 1
			shared.Telemetry().Logger.Debugf("validateOptions: A2A port not set, defaulting to %d", s.opts.A2A.Port)
		}
		if s.opts.A2A.Port < 1 || s.opts.A2A.Port > 65535 {
			return fmt.Errorf("A2A.Port must be between 1 and 65535")
		}
		shared.Telemetry().Logger.Debugf("validateOptions: A2A %s:%d transport=%s OK",
			s.opts.A2A.Host, s.opts.A2A.Port, s.opts.A2A.Transport)
	}

	if s.opts.ExperimentalHttp3 {
		shared.Telemetry().Logger.Debugf("validateOptions: HTTP/3 enabled, cert present=%v", s.cert != nil)
		if s.cert == nil {
			return fmt.Errorf("a TLS certificate is required when experimental HTTP/3 support is enabled")
		}
	}

	return nil
}

// mustLoadCertificate loads a TLS certificate and key from the specified files.
// It is intended for use during server initialization. If the certificate
// cannot be loaded, it logs a fatal error and terminates the application, as
// this is considered a critical configuration failure.
func mustLoadCertificate(certFile, keyFile string) tls.Certificate {
	shared.Telemetry().Logger.Debugf("mustLoadCertificate: loading cert=%s key=%s", certFile, keyFile)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		shared.Telemetry().Logger.Fatalf("Failed to load TLS certificate from certFile=%s, keyFile=%s: %v", certFile, keyFile, err)
	}
	shared.Telemetry().Logger.Debugf("mustLoadCertificate: certificate loaded OK")
	return cert
}

// getFromEnvOrDefault returns the value of the environment variable with the given key,
// or the default value if the environment variable is not set.
func getFromEnvOrDefault(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		shared.Telemetry().Logger.Debugf("getFromEnvOrDefault: %s not set, using default %q", key, defaultValue)
		return defaultValue
	}
	shared.Telemetry().Logger.Debugf("getFromEnvOrDefault: %s=%q", key, value)
	return value
}
