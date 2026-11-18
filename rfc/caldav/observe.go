package caldav

import (
	"net/http"
	"strings"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// WithSignals observes every request the handler answers, one server
// operation each: component "caldav", backend "http", named for the method
// (propfind, report, put and the others CalDAV uses, a fixed set, so a safe
// metric label), continuing the trace and baggage the client sent in its
// headers. The path is a span attribute, never a label. Without it, the
// handler reports through telemetry.Default, which
// runtime-go/observability's Adopt sets.
func WithSignals(s telemetry.Signals) Option {
	return func(h *Handler) { h.signals = s }
}

// observe runs serve as one operation of the request, when the handler has
// signals to report through.
func (h *Handler) observe(w http.ResponseWriter, r *http.Request, serve func(http.ResponseWriter, *http.Request)) {
	if h.obs == nil {
		serve(w, r)
		return
	}
	carrier := make(map[string]string, 3)
	for _, k := range []string{"traceparent", "tracestate", "baggage"} {
		if v := r.Header.Get(k); v != "" {
			carrier[k] = v
		}
	}
	ctx := h.obs.Tracer().Extract(r.Context(), carrier)
	ctx, op := h.obs.Start(ctx, "http", strings.ToLower(r.Method), telemetry.SpanKindServer,
		telemetry.WithSpanAttributes(map[string]any{
			"http.request.method": r.Method,
			"url.path":            r.URL.Path,
		}))
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	serve(rec, r.WithContext(ctx))
	op.Span().SetAttribute("http.response.status_code", rec.status)
	op.Finish(statusOutcome(rec.status))
}

// statusRecorder keeps the status a handler wrote.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the writer underneath.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// statusOutcome classifies a response status: the client's mistakes are not
// the server's failures.
func statusOutcome(code int) telemetry.Outcome {
	switch {
	case code < 400:
		return telemetry.OutcomeOK
	case code == http.StatusNotFound:
		return telemetry.OutcomeNotFound
	case code == http.StatusConflict, code == http.StatusPreconditionFailed:
		return telemetry.OutcomeConflict
	case code == http.StatusNotImplemented, code == http.StatusMethodNotAllowed:
		return telemetry.OutcomeUnsupported
	case code < 500:
		return telemetry.OutcomeInvalid
	default:
		return telemetry.OutcomeError
	}
}
