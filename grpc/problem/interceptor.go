package problem

import (
	"context"

	"google.golang.org/grpc"
)

// Interceptor translates every error a handler returns with ToStatus, so
// handlers can return their typed errors as they are.
func (t *Translator) Interceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			return resp, t.ToStatus(err)
		}
		return resp, nil
	}
}
