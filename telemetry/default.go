package telemetry

import "sync/atomic"

// defaults holds the process's default signals; see [SetDefault].
var defaults atomic.Pointer[Signals]

// SetDefault makes s the signals every runtime-go module reports through when
// it was given none of its own: a provider, a manager or a client built
// without explicit signals instruments itself with these. The application
// calls it once, in main, before building anything; runtime-go/observability's
// Adopt calls it with the application's SDK. Signals a caller passes
// explicitly always win, and the zero Signals turns the default off.
//
// Without it a module reports nothing unless it is handed signals: no runtime
// package starts an exporter or a pipeline of its own.
func SetDefault(s Signals) {
	defaults.Store(&s)
}

// Default returns the signals [SetDefault] set, or the zero Signals, which
// report nothing.
func Default() Signals {
	if s := defaults.Load(); s != nil {
		return *s
	}
	return Signals{}
}

// Or returns s when it carries any signal, and [Default] otherwise: what a
// module reports through, given what its caller passed.
func (s Signals) Or() Signals {
	if s.Enabled() {
		return s
	}
	return Default()
}
