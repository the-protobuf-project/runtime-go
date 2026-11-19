package repox

// grpc.go is the gRPC side of the reference contract that needs no generated
// stub: gRPC NOT_FOUND counts as not found, and the AIP Get that proves a
// resource exists, called by method name. The client and server of
// store.v1.ReferenceService, and the reference_services: store.yaml declares,
// are generated beside the repositories (repoxgrpc), as they are built on
// store's own stubs.

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func init() {
	known := IsNotFound
	IsNotFound = func(err error) bool { return known(err) || status.Code(err) == codes.NotFound }
}

// GRPCGetResolver resolves through the target's AIP Get (AIP-131) on conn,
// named by its full method ("vcard.v1.Cards/GetCard"), with no generated
// stub: the request is sent as a message holding the resource name in field
// 1, which is where AIP-131 puts it, and the response is read and dropped.
// NOT_FOUND means the resource does not exist; any other failure is returned.
// For a Get whose request differs, wrap the client's own call in GetResolver.
func GRPCGetResolver(conn grpc.ClientConnInterface, method string) ReferenceResolver {
	full := "/" + strings.TrimPrefix(method, "/")
	return ResolverFunc(func(ctx context.Context, _, name string) error {
		err := conn.Invoke(ctx, full, wrapperspb.String(name), new(emptypb.Empty))
		if status.Code(err) == codes.NotFound {
			return fmt.Errorf("%w: %v", ErrReferenceNotFound, err) //nolint:errorlint // the sentinel is the error's identity; the backend error is detail, deliberately not matchable
		}
		return err
	})
}
