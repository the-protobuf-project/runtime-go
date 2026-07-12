package grpc

import (
	"context"
	"fmt"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// NewValidationInterceptor returns a unary interceptor that checks every
// request message against its buf.validate rules (annotated in the protos and
// carried by the generated descriptors) before the handler runs. A violation
// maps to InvalidArgument with the rule's message, mirroring AIP error
// semantics.
//
// This covers message-scoped rules only — resource-name shapes, ranges, CEL
// field relations. Rules that need server state stay in the application.
// Exported (rather than only WithValidation) so in-process test servers can
// chain the same interceptor a production HybridServer runs.
func NewValidationInterceptor() (grpc.UnaryServerInterceptor, error) {
	validator, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("build protovalidate validator: %w", err)
	}
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if msg, ok := req.(proto.Message); ok {
			if err := validator.Validate(msg); err != nil {
				return nil, status.Error(codes.InvalidArgument, err.Error())
			}
		}
		return handler(ctx, req)
	}, nil
}

// NewStandaloneGRPCServer returns a plain gRPC server carrying the same
// validation-first unary interceptor chain a HybridServer runs with
// WithValidation — the seam for in-process (bufconn) test harnesses that need
// production-identical middleware without the hybrid transports.
func NewStandaloneGRPCServer(extra ...grpc.UnaryServerInterceptor) (*grpc.Server, error) {
	validate, err := NewValidationInterceptor()
	if err != nil {
		return nil, err
	}
	chain := append([]grpc.UnaryServerInterceptor{validate}, extra...)
	return grpc.NewServer(grpc.ChainUnaryInterceptor(chain...)), nil
}

// WithValidation enables protovalidate request validation for every unary RPC
// the server dispatches — gRPC, the HTTP gateway (which dials gRPC), and MCP
// tool calls alike. The interceptor is built during Start and prepended to the
// user-supplied chain, so validation always runs first; a validator build
// failure surfaces as a Start error.
func WithValidation() Option {
	return func(s *HybridServer) {
		s.enableValidation = true
	}
}
