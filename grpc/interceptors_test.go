package grpc

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/the-protobuf-project/runtime-go/agents/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
)

// recorder is a unary interceptor that notes its name and passes the call on.
func recorder(name string, seen *[]string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		*seen = append(*seen, name)
		return h(ctx, req)
	}
}

// runChain runs s's unary chain around a no-op handler and returns the order
// the interceptors ran in.
func runChain(t *testing.T, s *HybridServer, seen *[]string) []string {
	t.Helper()
	if err := s.resolveValidation(); err != nil {
		t.Fatalf("resolveValidation: %v", err)
	}
	if s.validate != nil {
		s.validate = recorder("validate", seen) // stands in for protovalidate
	}
	chain := mcp.ChainUnaryInterceptors(s.unaryChain()...)
	if _, err := chain(t.Context(), nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) { return "ok", nil }); err != nil {
		t.Fatalf("chain: %v", err)
	}
	return *seen
}

func TestInterceptorOrder(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts func(seen *[]string) []Option
		want []string
	}{
		{"validation first by default", func(seen *[]string) []Option {
			return []Option{WithValidation(), WithUnaryInterceptors(recorder("auth", seen))}
		}, []string{"validate", "auth"}},
		{"validation last", func(seen *[]string) []Option {
			return []Option{WithValidationLast(), WithUnaryInterceptors(recorder("auth", seen), recorder("audit", seen))}
		}, []string{"auth", "audit", "validate"}},
		{"no validation", func(seen *[]string) []Option {
			return []Option{WithUnaryInterceptors(recorder("auth", seen))}
		}, []string{"auth"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seen []string
			s := NewHybridServer(baseOpts(), tc.opts(&seen)...)
			if got := runChain(t, s, &seen); !slices.Equal(got, tc.want) {
				t.Errorf("ran %v, want %v", got, tc.want)
			}
			// A second resolve (Restart) must not add validation again.
			seen = nil
			if got := runChain(t, s, &seen); !slices.Equal(got, tc.want) {
				t.Errorf("after restart ran %v, want %v", got, tc.want)
			}
		})
	}
}

// requireToken refuses a call without authorization metadata.
func requireToken(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md.Get("authorization")) == 0 {
		return status.Error(codes.Unauthenticated, "no token")
	}
	return nil
}

// Stream interceptors guard the server's built-in streams — reflection here —
// alongside the unary chain guarding its unary methods.
func TestWithStreamInterceptors_GuardReflection(t *testing.T) {
	opts := mcpOpts(t)
	opts.EnableMCP = false
	opts.EnableHealth = true
	s := NewHybridServer(opts,
		WithValidationLast(),
		WithUnaryInterceptors(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			if err := requireToken(ctx); err != nil {
				return nil, err
			}
			return h(ctx, req)
		}),
		WithStreamInterceptors(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			if err := requireToken(ss.Context()); err != nil {
				return err
			}
			return h(srv, ss)
		}),
	)
	startHybrid(t, s)

	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", opts.GRPC.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	reflect := func(ctx context.Context) error {
		stream, err := reflectionpb.NewServerReflectionClient(conn).ServerReflectionInfo(ctx)
		if err != nil {
			return err
		}
		if sendErr := stream.Send(&reflectionpb.ServerReflectionRequest{
			MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{},
		}); sendErr != nil {
			return sendErr
		}
		_, err = stream.Recv()
		return err
	}
	check := func(ctx context.Context) error {
		_, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		return err
	}

	for name, call := range map[string]func(context.Context) error{"reflection": reflect, "health": check} {
		if err := call(t.Context()); status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s without a token: %v, want Unauthenticated", name, err)
		}
		authed := metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer x")
		if err := call(authed); err != nil {
			t.Errorf("%s with a token: %v", name, err)
		}
	}
}
