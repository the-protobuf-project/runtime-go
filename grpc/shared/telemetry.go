// Package shared holds the telemetry client the grpc package logs through.
//
// It is initialized on first use rather than in an init function, and a
// configuration failure degrades to a local-only logger instead of killing the
// process. A library cannot decide that its host binary should not start
// because an OTLP collector is unreachable.
package shared

import (
	"fmt"
	"os"
	"sync"

	"github.com/the-protobuf-project/opentelementry/opentelementry-go"
)

var (
	once      sync.Once
	telemetry *opentelementry.Opentelementry
)

// Telemetry returns the shared telemetry client, building it on first call.
//
// If the configured client cannot be built — no collector, a bad OTLP endpoint,
// an unwritable MCAP path — it falls back to one with the exporters disabled so
// logging still works locally. Only if that also fails does it return nil, and
// the failure is reported on stderr.
func Telemetry() *opentelementry.Opentelementry {
	once.Do(func() {
		o, err := opentelementry.New().
			WithService("runtime-go-grpc", "1.0.0").
			WithLogLevel(opentelementry.ModuleLevel_2).WithTracing().
			Build()
		if err == nil {
			telemetry = o
			return
		}

		// The exporters are what usually fail, and they are also what a library
		// can do without. Retry with the smallest configuration that still
		// gives the caller a logger.
		fmt.Fprintf(os.Stderr,
			"runtime-go/grpc: telemetry setup failed (%v); falling back to local logging\n", err)

		fallback, ferr := opentelementry.New().
			WithService("runtime-go-grpc", "1.0.0").
			WithLogLevel(opentelementry.ModuleLevel_2).
			Build()
		if ferr != nil {
			fmt.Fprintf(os.Stderr,
				"runtime-go/grpc: fallback telemetry also failed (%v); logging is disabled\n", ferr)
			return
		}
		telemetry = fallback
	})
	return telemetry
}

// Close releases the telemetry client. The main application should call it on
// shutdown. It is safe to call when telemetry was never initialized.
func Close() error {
	if telemetry != nil {
		return telemetry.Close()
	}
	return nil
}
