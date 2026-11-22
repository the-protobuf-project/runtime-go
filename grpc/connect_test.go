package grpc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// connectServer starts a HybridServer with health checking and Connect, and
// returns its HTTP base URL. intercept, when set, runs on every gRPC call.
func connectServer(t *testing.T, intercept grpc.UnaryServerInterceptor) string {
	t.Helper()
	opts := mcpOpts(t)
	opts.EnableMCP = false
	opts.EnableHTTP = true
	opts.EnableHealth = true
	extra := []Option{WithConnect()}
	if intercept != nil {
		extra = append(extra, WithUnaryInterceptors(intercept))
	}
	startHybrid(t, NewHybridServer(opts, extra...))
	return fmt.Sprintf("http://127.0.0.1:%d", opts.HTTP.Port)
}

// connectReply is what a test reads from a Connect response.
type connectReply struct {
	StatusCode int
	Header     http.Header
}

func connectPost(t *testing.T, url, contentType string, body []byte, header http.Header) (connectReply, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Connect-Protocol-Version", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return connectReply{StatusCode: resp.StatusCode, Header: resp.Header}, out
}

const healthCheck = "/grpc.health.v1.Health/Check"

func TestConnect_BinaryUnaryCall(t *testing.T) {
	base := connectServer(t, nil)
	req, _ := proto.Marshal(&grpc_health_v1.HealthCheckRequest{})
	resp, body := connectPost(t, base+healthCheck, "application/proto", req, nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/proto" {
		t.Fatalf("status %d, content type %q: %s", resp.StatusCode, resp.Header.Get("Content-Type"), body)
	}
	var out grpc_health_v1.HealthCheckResponse
	if err := proto.Unmarshal(body, &out); err != nil || out.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("got %v, %v; want SERVING", out.GetStatus(), err)
	}
}

func TestConnect_JSONUnaryCall(t *testing.T) {
	base := connectServer(t, nil)
	resp, body := connectPost(t, base+healthCheck, "application/json; charset=utf-8", []byte(`{}`), nil)
	if resp.StatusCode != http.StatusOK || string(bytes.TrimSpace(body)) != `{"status":"SERVING"}` {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
}

func TestConnect_ErrorsUseConnectCodes(t *testing.T) {
	base := connectServer(t, nil)
	cases := []struct {
		name, path, contentType, body string
		http                          int
		code                          string
	}{
		{"service error", healthCheck, "application/json", `{"service":"nope"}`, http.StatusNotFound, "not_found"},
		{"unknown method", "/grpc.health.v1.Health/Nope", "application/proto", "", http.StatusNotImplemented, "unimplemented"},
		{"unknown method, JSON", "/no.such.v1.Service/Method", "application/json", `{}`, http.StatusNotImplemented, "unimplemented"},
		{"bad JSON", healthCheck, "application/json", `{"service":`, http.StatusBadRequest, "invalid_argument"},
		{"bad content type", healthCheck, "text/plain", "", http.StatusUnsupportedMediaType, "unknown"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp, body := connectPost(t, base+c.path, c.contentType, []byte(c.body), nil)
			var got connectError
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatalf("error body %q: %v", body, err)
			}
			if resp.StatusCode != c.http || got.Code != c.code {
				t.Fatalf("got %d %q (%s), want %d %q", resp.StatusCode, got.Code, got.Message, c.http, c.code)
			}
		})
	}
}

// Connect calls run through the server's interceptors with the request's
// headers as metadata, and error details survive as base64 protobuf.
func TestConnect_MetadataInterceptorsAndDetails(t *testing.T) {
	var gotAuth []string
	base := connectServer(t, func(ctx context.Context, _ any, info *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		gotAuth = md.Get("authorization")
		_ = grpc.SetHeader(ctx, metadata.Pairs("x-served-by", info.FullMethod))
		st, _ := status.New(codes.FailedPrecondition, "hold expired").WithDetails(&errdetails.ErrorInfo{
			Reason: "HOLD_NOT_ACTIVE", Domain: "freebusy.example",
		})
		return nil, st.Err()
	})
	resp, body := connectPost(t, base+healthCheck, "application/json", []byte(`{}`),
		http.Header{"Authorization": {"Bearer token-1"}})
	if len(gotAuth) != 1 || gotAuth[0] != "Bearer token-1" {
		t.Fatalf("authorization metadata = %v", gotAuth)
	}
	if resp.Header.Get("X-Served-By") != healthCheck {
		t.Fatalf("response header x-served-by = %q", resp.Header.Get("X-Served-By"))
	}
	var got connectError
	if err := json.Unmarshal(body, &got); err != nil || got.Code != "failed_precondition" || len(got.Details) != 1 {
		t.Fatalf("error body %s (%v)", body, err)
	}
	raw, err := base64.RawStdEncoding.DecodeString(got.Details[0].Value)
	var info errdetails.ErrorInfo
	if err != nil || proto.Unmarshal(raw, &info) != nil || info.GetReason() != "HOLD_NOT_ACTIVE" ||
		got.Details[0].Type != "google.rpc.ErrorInfo" {
		t.Fatalf("detail %+v decodes to %v (%v)", got.Details[0], &info, err)
	}
}

// REST and /health keep working beside Connect, and GETs are not Connect.
func TestConnect_LeavesOtherRoutesAlone(t *testing.T) {
	base := connectServer(t, nil)
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, base+"/health", http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/health = %d", resp.StatusCode)
	}
	for path, want := range map[string]bool{
		healthCheck: true, "/v1/shelves/1": false, "/grpc.health.v1.Health": false, "/a.B/c/d": false, "/health/x": false,
	} {
		if got := isConnectPath(path); got != want {
			t.Errorf("isConnectPath(%q) = %v, want %v", path, got, want)
		}
	}
}

func TestConnect_OffByDefault(t *testing.T) {
	opts := mcpOpts(t)
	opts.EnableMCP, opts.EnableHTTP, opts.EnableHealth = false, true, true
	startHybrid(t, NewHybridServer(opts))
	resp, _ := connectPost(t, fmt.Sprintf("http://127.0.0.1:%d%s", opts.HTTP.Port, healthCheck), "application/json", []byte(`{}`), nil)
	if resp.StatusCode == http.StatusOK {
		t.Fatal("Connect answered without WithConnect")
	}
}

// The HTTP port speaks cleartext HTTP/2 (h2c) as well as HTTP/1.1, so a
// Connect client gets multiplexing and full-duplex streams without TLS.
func TestHTTPPort_ServesCleartextHTTP2(t *testing.T) {
	base := connectServer(t, nil)
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: &http.Transport{Protocols: protocols}}
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, base+healthCheck, bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("h2c POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusOK {
		t.Fatalf("got %s %d, want HTTP/2 200", resp.Proto, resp.StatusCode)
	}
}
