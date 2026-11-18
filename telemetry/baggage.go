package telemetry

import "context"

// Baggage is W3C baggage: request-scoped key/values that travel with the
// trace to every service downstream, in any language, through the same
// headers a [Tracer] injects and extracts. Use it for what every hop needs to
// know (a tenant, a request class), never for large or secret values: it
// rides on every outgoing message and request.
//
// An [Observer] copies the baggage an operation runs under onto its span and
// its completion log, as baggage.<key>. It is never a metric label: its
// values are unbounded.
type Baggage interface {
	// Set returns ctx carrying key=value beside the baggage it already has.
	Set(ctx context.Context, key, value string) (context.Context, error)
	// Members returns the baggage ctx carries, set here or extracted from a
	// caller's headers.
	Members(ctx context.Context) map[string]string
}

// NoopBaggage carries nothing: Set returns ctx unchanged. It is what a nil
// [Signals.Baggage] means.
var NoopBaggage Baggage = noopBaggage{}

type noopBaggage struct{}

func (noopBaggage) Set(ctx context.Context, _, _ string) (context.Context, error) { return ctx, nil }
func (noopBaggage) Members(context.Context) map[string]string                     { return nil }
