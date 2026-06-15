package grpc

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/the-protobuf-project/runtime-go/grpc/options"
	"github.com/the-protobuf-project/runtime-go/grpc/shared"
)

// applyEnvOverrides modifies the provided Options struct by applying settings
// found in environment variables. This allows for dynamic configuration of
// service endpoints without code changes, which is useful for different
// deployment environments (e.g., development, production).
//
// The environment variables must follow the pattern:
// {SERVICENAME}_HYBRID_{COMPONENT}_{SETTING}. For example, for a service
// named "vision", the gRPC port can be overridden by setting the
// VISION_HYBRID_GRPC_PORT environment variable.
func applyEnvOverrides(opts *options.Options) {
	prefix := fmt.Sprintf("%s_HYBRID", strings.ToUpper(opts.ServiceName))
	shared.Pulse.Logger.Debugf("applyEnvOverrides: scanning env with prefix %q", prefix)

	if host := os.Getenv(prefix + "_GRPC_HOST"); host != "" {
		shared.Pulse.Logger.Debugf("applyEnvOverrides: %s_GRPC_HOST=%q overrides gRPC host", prefix, host)
		opts.GRPC.Host = host
	}
	if portStr := os.Getenv(prefix + "_GRPC_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			shared.Pulse.Logger.Debugf("applyEnvOverrides: %s_GRPC_PORT=%d overrides gRPC port", prefix, port)
			opts.GRPC.Port = port
		} else {
			shared.Pulse.Logger.Debugf("applyEnvOverrides: %s_GRPC_PORT=%q is not a valid int, ignoring", prefix, portStr)
		}
	}
	if host := os.Getenv(prefix + "_HTTP_HOST"); host != "" {
		shared.Pulse.Logger.Debugf("applyEnvOverrides: %s_HTTP_HOST=%q overrides HTTP host", prefix, host)
		opts.HTTP.Host = host
	}
	if portStr := os.Getenv(prefix + "_HTTP_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			shared.Pulse.Logger.Debugf("applyEnvOverrides: %s_HTTP_PORT=%d overrides HTTP port", prefix, port)
			opts.HTTP.Port = port
		} else {
			shared.Pulse.Logger.Debugf("applyEnvOverrides: %s_HTTP_PORT=%q is not a valid int, ignoring", prefix, portStr)
		}
	}
	if host := os.Getenv(prefix + "_MCP_HOST"); host != "" {
		shared.Pulse.Logger.Debugf("applyEnvOverrides: %s_MCP_HOST=%q overrides MCP host", prefix, host)
		opts.MCP.Host = host
	}
	if portStr := os.Getenv(prefix + "_MCP_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			shared.Pulse.Logger.Debugf("applyEnvOverrides: %s_MCP_PORT=%d overrides MCP port", prefix, port)
			opts.MCP.Port = port
		} else {
			shared.Pulse.Logger.Debugf("applyEnvOverrides: %s_MCP_PORT=%q is not a valid int, ignoring", prefix, portStr)
		}
	}
}
