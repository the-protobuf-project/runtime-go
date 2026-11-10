package network

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
	"github.com/the-protobuf-project/runtime-go/telemetry/recorder"
)

// Every request is a client span, and the server receives its trace in the
// request's headers.
func TestARequestIsTracedToTheServer(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	rec := recorder.New()
	c := newPooledClient(time.Second, rec.Signals(), "http")
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/books/1", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	s := rec.Span("network.request")
	if s == nil || s.Kind != telemetry.SpanKindClient {
		t.Fatalf("span %+v", s)
	}
	if got != s.ID {
		t.Errorf("the server got traceparent %q, want the request span %q", got, s.ID)
	}
	if s.Attr("http.response.status_code") != http.StatusNotFound || s.Attr("url.path") != "/books/1" || s.Attr("http.request.method") != "GET" {
		t.Errorf("attributes %v %v %v", s.Attr("http.response.status_code"), s.Attr("url.path"), s.Attr("http.request.method"))
	}
	if out := rec.Outcomes()["request"]; out != "not_found" {
		t.Errorf("outcome %q", out)
	}
	if req.Header.Get("traceparent") != "" {
		t.Error("the transport modified the caller's request")
	}
}

func TestWithoutSignalsTheTransportIsPlain(t *testing.T) {
	c := newPooledClient(time.Second, telemetry.Signals{}, "http")
	if _, ok := c.Transport.(*http.Transport); !ok {
		t.Errorf("transport %T, want the plain pooled one", c.Transport)
	}
}

func TestStatusOutcomes(t *testing.T) {
	for code, want := range map[int]telemetry.Outcome{
		200: telemetry.OutcomeOK, 304: telemetry.OutcomeOK, 404: telemetry.OutcomeNotFound,
		409: telemetry.OutcomeConflict, 412: telemetry.OutcomeConflict, 400: telemetry.OutcomeInvalid,
		504: telemetry.OutcomeTimeout, 501: telemetry.OutcomeUnsupported, 500: telemetry.OutcomeError,
	} {
		if got := statusOutcome(code); got != want {
			t.Errorf("%d -> %v, want %v", code, got, want)
		}
	}
}
