package grpc

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/the-protobuf-project/runtime-go/grpc/shared"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// connectMaxBody bounds a Connect request body, matching gRPC's default
// maximum received message size so the two surfaces refuse the same payloads.
const connectMaxBody = 4 << 20

// WithConnect serves the Connect protocol's unary calls on the HTTP port,
// beside the REST gateway: POST /<package.Service>/<Method> with an
// application/proto or application/json body.
//
// Each call is forwarded to this server's own gRPC services over the
// in-memory listener the gateway uses, so interceptors, validation and
// telemetry run exactly as for a gRPC client. A binary call needs no type
// information and reaches every unary method, with or without a
// google.api.http binding; a JSON call resolves the method's message types
// from the global protobuf registry, which every generated package joins.
//
// Connect paths never clash with REST ones: a Connect path's first segment is
// a fully qualified service name and so contains a dot, which a REST route's
// does not. HTTP/1.1 and HTTP/3 serve it alike, behind CORS when configured.
// Streaming calls are not served yet.
func WithConnect() Option {
	return func(s *HybridServer) {
		shared.Telemetry().Logger.Debugf("WithConnect: serving Connect unary calls on the HTTP port")
		s.connect = true
	}
}

// connectBridge serves Connect unary calls by invoking the gRPC server
// in-process.
type connectBridge struct {
	conn *grpc.ClientConn
}

// isConnectPath reports whether path names a method, /pkg.Service/Method.
func isConnectPath(path string) bool {
	svc, method, ok := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	return ok && strings.Contains(svc, ".") && method != "" && !strings.Contains(method, "/")
}

// withConnect sends Connect calls to the bridge and everything else to next.
func (b *connectBridge) withConnect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && isConnectPath(r.URL.Path) {
			b.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (b *connectBridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	format, ok := connectFormat(r.Header.Get("Content-Type"))
	if !ok {
		w.Header().Set("Accept-Post", "application/proto, application/json")
		writeConnectError(w, http.StatusUnsupportedMediaType, status.New(codes.Unknown,
			"unsupported content type: send application/proto or application/json"), nil, nil)
		return
	}
	if enc := r.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		writeConnectError(w, 0, status.Newf(codes.Unimplemented, "content encoding %q is not supported", enc), nil, nil)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, connectMaxBody))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeConnectError(w, 0, status.Newf(codes.ResourceExhausted, "request body exceeds %d bytes", connectMaxBody), nil, nil)
			return
		}
		writeConnectError(w, 0, status.Newf(codes.InvalidArgument, "reading the request body: %v", err), nil, nil)
		return
	}

	ctx, cancel, err := connectContext(r)
	if err != nil {
		writeConnectError(w, 0, status.New(codes.InvalidArgument, err.Error()), nil, nil)
		return
	}
	defer cancel()

	method := r.URL.Path
	req, err := format.request(method, body)
	if err != nil {
		writeConnectError(w, 0, status.Convert(err), nil, nil)
		return
	}
	var resp []byte
	var header, trailer metadata.MD
	err = b.conn.Invoke(ctx, method, req, &resp, grpc.ForceCodecV2(rawCodec{}), grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		writeConnectError(w, 0, status.Convert(err), header, trailer)
		return
	}
	out, err := format.response(method, resp)
	if err != nil {
		writeConnectError(w, 0, status.Convert(err), header, trailer)
		return
	}
	writeMetadata(w.Header(), header, "")
	writeMetadata(w.Header(), trailer, "Trailer-")
	w.Header().Set("Content-Type", format.contentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

// connectContext carries the request's metadata and Connect-Timeout-Ms into
// the gRPC call.
func connectContext(r *http.Request) (context.Context, context.CancelFunc, error) {
	base := metadata.NewOutgoingContext(r.Context(), requestMetadata(r.Header))
	raw := r.Header.Get("Connect-Timeout-Ms")
	if raw == "" {
		ctx, cancel := context.WithCancel(base)
		return ctx, cancel, nil
	}
	ms, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || ms < 0 || len(raw) > 10 {
		return nil, nil, errors.New("Connect-Timeout-Ms must be a positive integer of at most 10 digits")
	}
	ctx, cancel := context.WithTimeout(base, time.Duration(ms)*time.Millisecond)
	return ctx, cancel, nil
}
