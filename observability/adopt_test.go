package observability

import (
	"testing"

	"github.com/the-protobuf-project/telemetry/telemetry-go"
)

// Once an application adopts its SDK instance, every module's lazy accessor
// returns a Client over that instance -- including one that already built its
// own -- and closing it leaves the application's instance alone.
func TestAdopt(t *testing.T) {
	t.Cleanup(func() { adopted.Store(nil) })

	early := Lazy("early-module", "1.0.0")
	own := early()

	sdk, err := telemetry.New().WithService("app", "1.0.0").Build()
	if err != nil {
		t.Fatal(err)
	}
	// Not closed: with no collector the SDK's shutdown waits 20 seconds on
	// the OTLP exporter it configures by default (see Options.CloseTimeout).
	Adopt(sdk)

	for name, get := range map[string]func() *Client{"built before": early, "built after": Lazy("late-module", "1.0.0")} {
		c := get()
		if c.Otel() != sdk {
			t.Errorf("%s Adopt: the accessor does not return the application's instance", name)
		}
		if err := c.Close(); err != nil {
			t.Errorf("%s Adopt: Close: %v", name, err)
		}
	}
	if own != nil && own.Otel() == sdk {
		t.Error("the module's own client was replaced in place")
	}
	// Closing the adopted Client must not have shut the application's down:
	// its logger still takes records.
	sdk.Logger.Info("still open")

	Adopt(nil) // ignored
	if early().Otel() != sdk {
		t.Error("Adopt(nil) dropped the adopted instance")
	}
}
