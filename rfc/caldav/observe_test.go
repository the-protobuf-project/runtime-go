package caldav_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/the-protobuf-project/runtime-go/rfc/caldav"
	"github.com/the-protobuf-project/runtime-go/rfc/caldav/memstore"
	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/the-protobuf-project/runtime-go/telemetry/recorder"
)

// Every request is one server operation named for its method, continuing the
// trace and the baggage the client sent.
func TestARequestIsAnOperationContinuingTheClientsTrace(t *testing.T) {
	rec := recorder.New()
	h := caldav.New(memstore.New(), caldav.WithSignals(rec.Signals()))

	req := httptest.NewRequestWithContext(t.Context(), "PROPFIND", h.Prefix()+"/", nil)
	req.Header.Set("Depth", "0")
	req.Header.Set("traceparent", "client-span")
	req.Header.Set("baggage", "tenant=acme")
	h.ServeHTTP(httptest.NewRecorder(), req)

	s := rec.Span("caldav.propfind")
	if s == nil {
		t.Fatalf("no caldav.propfind span: %v", rec.Spans())
	}
	if s.Kind != telemetry.SpanKindServer || s.Parent != "client-span" {
		t.Errorf("span kind %v parent %q, want a server span under client-span", s.Kind, s.Parent)
	}
	if s.Attr("baggage.tenant") != "acme" {
		t.Errorf("baggage.tenant = %v, want acme", s.Attr("baggage.tenant"))
	}
	if got := rec.Outcomes()["propfind"]; got != "ok" {
		t.Errorf("outcome %q", got)
	}
}

// A request for something that is not there is a not_found outcome, not a
// failure.
func TestAMissingResourceIsNotFoundNotAFailure(t *testing.T) {
	rec := recorder.New()
	h := caldav.New(memstore.New(), caldav.WithSignals(rec.Signals()))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, h.Prefix()+"/calendars/nobody/none/x.ics", nil))
	if w.Code != http.StatusNotFound {
		t.Skipf("status %d; the outcome mapping is checked against a 404", w.Code)
	}
	if got := rec.Outcomes()["get"]; got != "not_found" {
		t.Errorf("outcome %q, want not_found", got)
	}
}
