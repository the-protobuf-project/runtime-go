// Package observability is the single telemetry surface for runtime-go:
// logging, metrics and tracing, plus the wiring that binds them to a backend.
//
// Every other runtime-go module imports this package and nothing else. It
// re-exports the backend-agnostic contracts — [Logger], [Meter], [Tracer] and
// their instruments — as type aliases, so there is one name for each concept
// rather than one in a contract package and another here.
//
// # Use
//
// Set the backend up once, in main, and every runtime-go module that
// instruments itself starts reporting through it:
//
//	obs := observability.Must("my-app", "1.0.0")
//	observability.SetDefault(obs)
//	defer obs.Close()
//
// Until [SetDefault] is called the default is [Noop]: instrumentation costs
// nothing and records nothing, so importing a runtime-go module never forces a
// telemetry backend on a consumer that did not ask for one.
//
// Read the default through [Log], [Trace] and [Default]:
//
//	observability.Log().Info(ctx, "cache warmed", observability.Fields{"keys": n})
//
//	err := observability.Trace(ctx, "cache.Get", func(ctx context.Context, sp observability.Span) error {
//	    sp.SetAttributes(observability.Fields{"key": key})
//	    return c.Get(ctx, key, &out)
//	})
//
// A caller that wants an explicit instance rather than the process default can
// hold a [Client] and pass it, since [Client] implements [Telemetry].
//
// # Instruments
//
// Instruments follow a create-once, record-many pattern: resolve the handle
// once (in a constructor) and hold it, rather than re-resolving per
// measurement.
//
//	m := observability.Default().Meter()
//	hits := m.Counter("cache_hits_total", observability.WithUnit("1"))
//	hits.Add(ctx, 1, observability.Labels{"backend": "redis"})
//
// # Where the contract types come from
//
// [Logger], [Meter] and the instrument types are aliases for the types in
// github.com/the-protobuf-project/runtime-go/telemetry, a module that is no
// longer developed in this repository — it is consumed from the module proxy
// at a pinned version, like any other dependency.
//
// It still exists because the telemetry SDK builds its [Meter] from unexported
// and internal state, so only the SDK can supply that bridge, and it binds to
// that module by import path. Aliasing keeps that an implementation detail:
// observability.Meter and telemetry.Meter are the same type, so nothing in
// runtime-go needs to name the older module. Import this package.
package observability
