package network

import (
	"net/http"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// newPooledClient returns an http.Client with a dedicated pooled transport.
// A client built without a Transport shares http.DefaultTransport, which keeps
// only 2 idle connections per host — so concurrent requests to one endpoint
// (the normal shape for a GraphQL or REST backend) constantly open and close
// TCP connections. A dedicated transport sized for single-host traffic lets
// the pool actually reuse connections, and keeps this module's clients from
// contending on the process-wide default transport.
//
// With signals, every request through it is traced as backend.
func newPooledClient(timeout time.Duration, signals telemetry.Signals, backend string) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 100
	var rt http.RoundTripper = transport
	if signals.Enabled() {
		rt = &observedTransport{next: transport, obs: signals.Observer("network"), backend: backend}
	}
	return &http.Client{Timeout: timeout, Transport: rt}
}

// observedTransport makes every request one client operation, network.request,
// and carries its trace to the server in the request's headers.
type observedTransport struct {
	next    http.RoundTripper
	obs     *telemetry.Observer
	backend string
}

func (t *observedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, op := t.obs.Start(req.Context(), t.backend, "request", telemetry.SpanKindClient,
		telemetry.WithSpanAttributes(map[string]any{
			"http.request.method": req.Method,
			"server.address":      req.URL.Hostname(),
			"url.path":            req.URL.Path,
		}))

	// A RoundTripper must not modify the request it is given, so the trace
	// headers go on a copy.
	out := req.Clone(ctx)
	trace := map[string]string{}
	t.obs.Tracer().Inject(ctx, trace)
	for k, v := range trace {
		out.Header.Set(k, v)
	}

	resp, err := t.next.RoundTrip(out)
	if err != nil {
		op.FinishErr(err)
		return nil, err
	}
	op.Span().SetAttribute("http.response.status_code", resp.StatusCode)
	op.Finish(statusOutcome(resp.StatusCode))
	return resp, nil
}

// statusOutcome classifies an HTTP status: a 404 is a missing record, a 409 or
// 412 a conflict, any other 4xx the caller's input, a 5xx the server's error.
func statusOutcome(code int) telemetry.Outcome {
	switch {
	case code < 400:
		return telemetry.OutcomeOK
	case code == http.StatusNotFound:
		return telemetry.OutcomeNotFound
	case code == http.StatusConflict, code == http.StatusPreconditionFailed:
		return telemetry.OutcomeConflict
	case code == http.StatusRequestTimeout, code == http.StatusGatewayTimeout:
		return telemetry.OutcomeTimeout
	case code == http.StatusNotImplemented:
		return telemetry.OutcomeUnsupported
	case code < 500:
		return telemetry.OutcomeInvalid
	default:
		return telemetry.OutcomeError
	}
}
