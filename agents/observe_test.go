package agents

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/the-protobuf-project/runtime-go/telemetry/recorder"
)

func TestARequestIsAServerSpanUnderTheCallersTrace(t *testing.T) {
	rec := recorder.New()
	mux := http.NewServeMux()
	var inner string
	mux.HandleFunc("POST /mcp/", func(w http.ResponseWriter, r *http.Request) {
		inner = recorder.SpanID(r.Context())
		w.WriteHeader(http.StatusAccepted)
	})
	h := observeHTTP(mux, rec.Signals())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/mcp/tools", nil)
	req.Header.Set("traceparent", "caller-span")
	h.ServeHTTP(httptest.NewRecorder(), req)

	s := rec.Span("agents.request")
	if s == nil || s.Kind != telemetry.SpanKindServer || s.Parent != "caller-span" {
		t.Fatalf("span %+v", s)
	}
	if inner != s.ID {
		t.Error("the handler does not run under the request's span")
	}
	if s.Attr("http.route") != "POST /mcp/" || s.Attr("http.response.status_code") != http.StatusAccepted {
		t.Errorf("attributes route %v status %v", s.Attr("http.route"), s.Attr("http.response.status_code"))
	}
}

func TestWithoutSignalsTheHandlerIsUnwrapped(t *testing.T) {
	mux := http.NewServeMux()
	if observeHTTP(mux, telemetry.Signals{}) != http.Handler(mux) {
		t.Fatal("observeHTTP without signals should return the handler itself")
	}
}
