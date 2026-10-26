package grpc

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/grpc/options"
)

// corsServe runs one request through p wrapped around a handler that records
// whether it was reached and answers 200.
func corsServe(t *testing.T, p CORSPolicy, method, origin string, headers map[string]string) (*httptest.ResponseRecorder, bool) {
	t.Helper()
	mw, err := CORS(p)
	if err != nil {
		t.Fatalf("CORS: %v", err)
	}
	reached := false
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequestWithContext(t.Context(), method, "/v1/things", nil)
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec, reached
}

func preflightHeaders(method string) map[string]string {
	return map[string]string{
		"Access-Control-Request-Method":  method,
		"Access-Control-Request-Headers": "content-type, x-goog-api-client",
	}
}

func hasVaryOrigin(rec *httptest.ResponseRecorder) bool {
	for _, v := range rec.Header().Values("Vary") {
		for part := range strings.SplitSeq(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), "Origin") {
				return true
			}
		}
	}
	return false
}

func TestCORS_PreflightFromAllowedOrigin(t *testing.T) {
	p := CORSPolicy{AllowedOrigins: []string{"https://app.example.com"}, MaxAge: 10 * time.Minute}
	rec, reached := corsServe(t, p, http.MethodOptions, "https://app.example.com", preflightHeaders(http.MethodPatch))

	if reached {
		t.Fatal("a preflight must not reach the handler")
	}
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	h := rec.Header()
	if got := h.Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Errorf("Allow-Origin = %q", got)
	}
	if got := h.Get("Access-Control-Allow-Methods"); got != "GET, POST, PATCH, PUT, DELETE, OPTIONS" {
		t.Errorf("Allow-Methods = %q", got)
	}
	allowed := strings.ToLower(h.Get("Access-Control-Allow-Headers"))
	for _, want := range []string{"content-type", "authorization", "x-goog-api-client", "x-goog-request-params", "x-goog-user-project", "x-api-key"} {
		if !strings.Contains(allowed, want) {
			t.Errorf("Allow-Headers %q lacks %s", allowed, want)
		}
	}
	if got := h.Get("Access-Control-Max-Age"); got != "600" {
		t.Errorf("Max-Age = %q, want 600", got)
	}
	if got := h.Get("Access-Control-Allow-Credentials"); got != "" {
		t.Errorf("Allow-Credentials = %q, want none", got)
	}
	if !hasVaryOrigin(rec) {
		t.Errorf("Vary = %v, want Origin", h.Values("Vary"))
	}
}

func TestCORS_PreflightRefused(t *testing.T) {
	p := CORSPolicy{AllowedOrigins: []string{"https://app.example.com"}, AllowedMethods: []string{"GET"}}
	for name, tc := range map[string]struct{ origin, method string }{
		"disallowed origin": {"https://evil.example.com", http.MethodGet},
		"disallowed method": {"https://app.example.com", http.MethodDelete},
	} {
		t.Run(name, func(t *testing.T) {
			rec, reached := corsServe(t, p, http.MethodOptions, tc.origin, preflightHeaders(tc.method))
			if reached {
				t.Fatal("a refused preflight must not reach the handler")
			}
			if rec.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want 403", rec.Code)
			}
			if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
				t.Errorf("Allow-Origin = %q, want none", got)
			}
			if !hasVaryOrigin(rec) {
				t.Error("Vary: Origin missing")
			}
		})
	}
}

func TestCORS_SimpleRequests(t *testing.T) {
	p := CORSPolicy{
		AllowedOrigins: []string{"https://app.example.com"},
		ExposedHeaders: []string{"X-Request-Id", "Grpc-Metadata-Trace"},
	}

	rec, reached := corsServe(t, p, http.MethodGet, "https://app.example.com", nil)
	if !reached || rec.Code != http.StatusOK {
		t.Fatalf("allowed GET: reached=%t status=%d", reached, rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Errorf("Allow-Origin = %q", got)
	}
	if got := rec.Header().Get("Access-Control-Expose-Headers"); got != "X-Request-Id, Grpc-Metadata-Trace" {
		t.Errorf("Expose-Headers = %q", got)
	}
	if !hasVaryOrigin(rec) {
		t.Error("Vary: Origin missing")
	}

	rec, reached = corsServe(t, p, http.MethodGet, "https://evil.example.com", nil)
	if !reached {
		t.Fatal("a disallowed simple request still reaches the handler; the browser withholds the answer")
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("disallowed origin got Allow-Origin %q", got)
	}
	if !hasVaryOrigin(rec) {
		t.Error("Vary: Origin missing on a disallowed origin")
	}

	rec, reached = corsServe(t, p, http.MethodGet, "", nil)
	if !reached || rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("same-origin request: reached=%t Allow-Origin=%q", reached, rec.Header().Get("Access-Control-Allow-Origin"))
	}

	// OPTIONS without Access-Control-Request-Method is not a preflight.
	_, reached = corsServe(t, p, http.MethodOptions, "https://app.example.com", nil)
	if !reached {
		t.Error("a plain OPTIONS request should reach the handler")
	}
}

func TestCORS_OriginMatching(t *testing.T) {
	p := CORSPolicy{AllowedOrigins: []string{"https://App.Example.com/", "http://localhost:5173"}}
	for origin, want := range map[string]bool{
		"https://app.example.com":      true,
		"HTTPS://APP.EXAMPLE.COM":      true,
		"http://localhost:5173":        true,
		"http://localhost:5174":        false,
		"http://app.example.com":       false,
		"https://app.example.com.evil": false,
		"https://sub.app.example.com":  false,
	} {
		rec, _ := corsServe(t, p, http.MethodGet, origin, nil)
		if got := rec.Header().Get("Access-Control-Allow-Origin") != ""; got != want {
			t.Errorf("origin %q allowed = %t, want %t", origin, got, want)
		}
	}
}

func TestCORS_Wildcard(t *testing.T) {
	rec, _ := corsServe(t, CORSPolicy{AllowedOrigins: []string{"*"}}, http.MethodGet, "https://anything.example", nil)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Allow-Origin = %q, want *", got)
	}

	p := CORSPolicy{AllowedOrigins: []string{"*"}, AllowedHeaders: []string{"*"}}
	rec, _ = corsServe(t, p, http.MethodOptions, "https://anything.example", map[string]string{
		"Access-Control-Request-Method":  "POST",
		"Access-Control-Request-Headers": "x-custom, content-type",
	})
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "x-custom, content-type" {
		t.Errorf("wildcard headers should echo the request, got %q", got)
	}
}

func TestCORS_Credentials(t *testing.T) {
	p := CORSPolicy{AllowedOrigins: []string{"https://app.example.com"}, AllowCredentials: true}
	rec, _ := corsServe(t, p, http.MethodGet, "https://app.example.com", nil)
	if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Errorf("Allow-Credentials = %q, want true", got)
	}
	rec, _ = corsServe(t, p, http.MethodOptions, "https://app.example.com", preflightHeaders("POST"))
	if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Errorf("preflight Allow-Credentials = %q, want true", got)
	}
}

func TestCORSPolicy_Validate(t *testing.T) {
	for name, tc := range map[string]struct {
		p       CORSPolicy
		wantErr string
	}{
		"ok":                      {CORSPolicy{AllowedOrigins: []string{"https://a.example", "http://localhost:3000"}}, ""},
		"wildcard":                {CORSPolicy{AllowedOrigins: []string{"*"}}, ""},
		"no origins":              {CORSPolicy{}, "no allowed origins"},
		"wildcard + credentials":  {CORSPolicy{AllowedOrigins: []string{"*"}, AllowCredentials: true}, "cannot be combined"},
		"path":                    {CORSPolicy{AllowedOrigins: []string{"https://a.example/app"}}, "scheme://host"},
		"no scheme":               {CORSPolicy{AllowedOrigins: []string{"a.example"}}, "scheme://host"},
		"negative max age":        {CORSPolicy{AllowedOrigins: []string{"*"}, MaxAge: -time.Second}, "negative"},
		"credentials with origin": {CORSPolicy{AllowedOrigins: []string{"https://a.example"}, AllowCredentials: true}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			err := tc.p.Validate()
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
	if _, err := CORS(CORSPolicy{AllowedOrigins: []string{"*"}, AllowCredentials: true}); err == nil {
		t.Fatal("CORS must refuse * with credentials")
	}
}

func TestWithCORS_WrapsServeHandler(t *testing.T) {
	s := NewHybridServer(options.Options{ServiceName: "test"},
		WithHTTPHandler(stubHandler()),
		WithCORS(CORSPolicy{AllowedOrigins: []string{"https://app.example.com"}}))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodOptions, "/v1/artists", nil)
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	rec := httptest.NewRecorder()
	s.serveHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent || rec.Header().Get("Access-Control-Allow-Origin") != "https://app.example.com" {
		t.Fatalf("preflight through serveHandler: status=%d headers=%v", rec.Code, rec.Header())
	}

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health", nil)
	req.Header.Set("Origin", "https://app.example.com")
	rec = httptest.NewRecorder()
	s.serveHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Header().Get("Access-Control-Allow-Origin") == "" {
		t.Fatalf("/health from an allowed origin: status=%d headers=%v", rec.Code, rec.Header())
	}
}

func TestWithCORS_InvalidPolicyFailsStart(t *testing.T) {
	s := NewHybridServer(options.Options{ServiceName: "test", GRPC: options.GRPCOptions{Host: "127.0.0.1", Port: 1}},
		WithCORS(CORSPolicy{AllowedOrigins: []string{"*"}, AllowCredentials: true}))
	if err := s.validateOptions(); err == nil || !strings.Contains(err.Error(), "cors") {
		t.Fatalf("validateOptions = %v, want a cors error", err)
	}
}

func TestDefaultCORSLists_AreFresh(t *testing.T) {
	m := DefaultCORSAllowedMethods()
	m[0] = "BOGUS"
	if slices.Contains(DefaultCORSAllowedMethods(), "BOGUS") {
		t.Fatal("DefaultCORSAllowedMethods must return a fresh slice")
	}
}
