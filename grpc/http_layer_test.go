package grpc

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/the-protobuf-project/runtime-go/grpc/options"
)

// stubHandler stands in for a generated HTTP/JSON route table: it answers its own absolute
// paths and 404s everything else, which is what transcode's handler does.
func stubHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/artists", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"artists":[]}`))
	})
	return mux
}

func TestServeHandler_DefaultsToTheGatewayMux(t *testing.T) {
	s := NewHybridServer(options.Options{ServiceName: "test"})
	if got := s.serveHandler(); got != http.Handler(s.mux) {
		t.Fatalf("without WithHTTPHandler the gateway mux should be served, got %T", got)
	}
}

func TestServeHandler_MountsTheSuppliedHandler(t *testing.T) {
	s := NewHybridServer(options.Options{ServiceName: "test"}, WithHTTPHandler(stubHandler()))

	rec := httptest.NewRecorder()
	s.serveHandler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/artists", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("generated route should answer, got %d", rec.Code)
	}
	if body := rec.Body.String(); body != `{"artists":[]}` {
		t.Fatalf("unexpected body %q", body)
	}
}

func TestServeHandler_KeepsHealthAlongsideIt(t *testing.T) {
	s := NewHybridServer(options.Options{ServiceName: "test"}, WithHTTPHandler(stubHandler()))

	rec := httptest.NewRecorder()
	s.serveHandler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("/health must survive a replaced gateway, got %d", rec.Code)
	}
	var body struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("health body is not JSON: %v", err)
	}
	if body.Status != "ok" {
		t.Fatalf("status = %q, want ok", body.Status)
	}
	if body.Message == "" {
		t.Fatal("health message should not be empty")
	}
}

func TestServeHandler_UnroutedPathIsNotFound(t *testing.T) {
	s := NewHybridServer(options.Options{ServiceName: "test"}, WithHTTPHandler(stubHandler()))

	rec := httptest.NewRecorder()
	s.serveHandler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/nothing", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("an unrouted path should 404, got %d", rec.Code)
	}
}

func TestServeHandler_IsTheSameForEveryTransport(t *testing.T) {
	// HTTP/1.1 and HTTP/3 both call serveHandler, so a request routes identically whichever
	// listener read it. Asserting they agree is what keeps that true as the layers change.
	s := NewHybridServer(options.Options{ServiceName: "test"}, WithHTTPHandler(stubHandler()))

	for _, path := range []string{"/v1/artists", "/health", "/v1/nothing"} {
		first := httptest.NewRecorder()
		s.serveHandler().ServeHTTP(first, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))

		second := httptest.NewRecorder()
		s.serveHandler().ServeHTTP(second, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))

		if first.Code != second.Code {
			t.Fatalf("%s: transports disagree (%d vs %d)", path, first.Code, second.Code)
		}
	}
}
