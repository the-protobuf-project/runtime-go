package agents

import (
	"net/http"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// observeHTTP wraps next so every request is one server operation,
// agents.request, under the trace the caller sent in its headers; see
// [Config.Signals]. Without signals it returns next.
func observeHTTP(next http.Handler, signals telemetry.Signals) http.Handler {
	if !signals.Enabled() {
		return next
	}
	obs := signals.Observer("agents")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		carrier := make(map[string]string, 2)
		for _, k := range []string{"traceparent", "tracestate", "baggage"} {
			if v := r.Header.Get(k); v != "" {
				carrier[k] = v
			}
		}
		ctx := obs.Tracer().Extract(r.Context(), carrier)
		ctx, op := obs.Start(ctx, "http", "request", telemetry.SpanKindServer,
			telemetry.WithSpanAttributes(map[string]any{
				"http.request.method": r.Method,
				"url.path":            r.URL.Path,
			}))
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		req := r.WithContext(ctx)
		next.ServeHTTP(rec, req)
		// ServeMux records the pattern it routed by on the request it served.
		if req.Pattern != "" {
			op.Span().SetAttribute("http.route", req.Pattern)
		}
		op.Span().SetAttribute("http.response.status_code", rec.status)
		op.Finish(statusOutcome(rec.status))
	})
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

// Flush passes a streaming handler's flush on, so server-sent events and
// streamable HTTP keep working through the wrapper.
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the writer underneath.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// statusOutcome classifies a response status the server sent: the caller's
// mistakes are not the server's failures.
func statusOutcome(code int) telemetry.Outcome {
	switch {
	case code < 400:
		return telemetry.OutcomeOK
	case code == http.StatusNotFound:
		return telemetry.OutcomeNotFound
	case code == http.StatusConflict, code == http.StatusPreconditionFailed:
		return telemetry.OutcomeConflict
	case code == http.StatusNotImplemented:
		return telemetry.OutcomeUnsupported
	case code < 500:
		return telemetry.OutcomeInvalid
	default:
		return telemetry.OutcomeError
	}
}
