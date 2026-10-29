package grpc

import (
	"slices"

	"github.com/the-protobuf-project/runtime-go/agents/mcp"
	"github.com/the-protobuf-project/runtime-go/grpc/shared"
	"google.golang.org/grpc"
)

// WithStreamInterceptors returns a server Option that installs one or more
// stream server interceptors on the gRPC server, chained in the order given.
// They guard every streaming RPC the server answers — the caller's own
// streaming methods, and the built-in ones such as gRPC reflection — which
// unary interceptors never see.
//
// MCP tool dispatch is unary only and does not run them.
func WithStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) Option {
	return func(s *HybridServer) {
		shared.Telemetry().Logger.Debugf("WithStreamInterceptors: appending %d stream interceptor(s)", len(interceptors))
		s.streamInts = append(s.streamInts, interceptors...)
	}
}

// WithValidationLast enables protovalidate request validation, as
// [WithValidation] does, but runs it after the interceptors given to
// [WithUnaryInterceptors] rather than before them.
//
// Use it when one of those interceptors decides whether the caller may ask at
// all — authentication above all: a request without credentials is then
// answered Unauthenticated, and only an admitted one learns what is wrong with
// its fields. [WithValidation] keeps validation first, as it always has.
// Given both, validation runs last.
func WithValidationLast() Option {
	return func(s *HybridServer) {
		s.enableValidation = true
		s.validationLast = true
	}
}

// WithMCPHeaderMappings sets the HTTP headers of each MCP request that reach
// the unary interceptor chain as incoming gRPC metadata, replacing the
// default, [mcp.DefaultHeaderMappings] (Authorization, X-Request-Id and
// X-Trace-Id). It applies to the [WithMCPEndpoint] path and to each service's
// own path alike, with or without sessions. Called with no mappings, it
// forwards no header.
func WithMCPHeaderMappings(mappings ...mcp.HeaderMapping) Option {
	return func(s *HybridServer) {
		shared.Telemetry().Logger.Debugf("WithMCPHeaderMappings: forwarding %d MCP header(s)", len(mappings))
		s.mcpHeaders = slices.Clone(mappings)
		if s.mcpHeaders == nil {
			s.mcpHeaders = []mcp.HeaderMapping{}
		}
	}
}

// mcpHeaderMappings is what WithMCPHeaderMappings set, or the defaults.
func (s *HybridServer) mcpHeaderMappings() []mcp.HeaderMapping {
	if s.mcpHeaders != nil {
		return s.mcpHeaders
	}
	return mcp.DefaultHeaderMappings()
}

// resolveValidation builds the protovalidate interceptor once, the first time
// the server starts with validation enabled; Restart reuses it.
func (s *HybridServer) resolveValidation() error {
	if !s.enableValidation || s.validate != nil {
		return nil
	}
	validate, err := NewValidationInterceptor()
	if err != nil {
		return err
	}
	s.validate = validate
	return nil
}

// unaryChain is the unary interceptor chain the gRPC server and MCP tool
// dispatch both run: the caller's interceptors, with validation (when
// enabled) first or, after WithValidationLast, last. It is computed afresh
// rather than stored, so a Restart never adds validation twice.
func (s *HybridServer) unaryChain() []grpc.UnaryServerInterceptor {
	if s.validate == nil {
		return s.unaryInts
	}
	if s.validationLast {
		return append(slices.Clone(s.unaryInts), s.validate)
	}
	return append([]grpc.UnaryServerInterceptor{s.validate}, s.unaryInts...)
}

// interceptorOptions is the gRPC server options installing both chains.
func (s *HybridServer) interceptorOptions() []grpc.ServerOption {
	var opts []grpc.ServerOption
	if chain := s.unaryChain(); len(chain) > 0 {
		shared.Telemetry().Logger.Debugf("gRPC: chaining %d unary interceptor(s)", len(chain))
		opts = append(opts, grpc.ChainUnaryInterceptor(chain...))
	}
	if len(s.streamInts) > 0 {
		shared.Telemetry().Logger.Debugf("gRPC: chaining %d stream interceptor(s)", len(s.streamInts))
		opts = append(opts, grpc.ChainStreamInterceptor(s.streamInts...))
	}
	return opts
}
