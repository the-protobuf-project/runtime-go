package cache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/the-protobuf-project/runtime-go/telemetry"
)

// fakeVolatile answers Get with the scripted error; everything else succeeds.
type fakeVolatile struct{ getErr error }

func (f fakeVolatile) Set(context.Context, string, any, ...Option) error { return nil }
func (f fakeVolatile) Get(context.Context, string, any) error            { return f.getErr }
func (f fakeVolatile) Delete(context.Context, string) error              { return nil }
func (f fakeVolatile) Touch(context.Context, string, time.Duration) error {
	return nil
}
func (f fakeVolatile) TTL(context.Context, string) (time.Duration, error) { return 0, nil }
func (f fakeVolatile) Scan(context.Context, string) ([]string, error)     { return nil, nil }

// recordingMeter keeps what its counters were told, by instrument and label set.
type recordingMeter struct {
	telemetry.Meter
	mu     sync.Mutex
	counts map[string]float64
}

type recordingCounter struct {
	m    *recordingMeter
	name string
}

func (c recordingCounter) Add(_ context.Context, delta float64, labels telemetry.Labels) {
	c.m.mu.Lock()
	defer c.m.mu.Unlock()
	key := c.name
	for _, k := range []string{"operation", "outcome", "result", "strategy"} {
		if v, ok := labels[k]; ok {
			key += " " + k + "=" + v
		}
	}
	c.m.counts[key] += delta
}

func (m *recordingMeter) Counter(name string, _ ...telemetry.InstrumentOption) telemetry.Counter {
	return recordingCounter{m: m, name: name}
}

func TestWithVolatileTelemetryCountsHitsAndMisses(t *testing.T) {
	m := &recordingMeter{Meter: telemetry.NoopMeter, counts: map[string]float64{}}
	for _, err := range []error{nil, nil, ErrNotFound, errors.New("down")} {
		v := WithVolatileTelemetry(fakeVolatile{getErr: err}, m)
		if got := v.Get(t.Context(), "k", &struct{}{}); !errors.Is(got, err) {
			t.Fatalf("Get returned %v, want %v unchanged", got, err)
		}
	}
	for key, want := range map[string]float64{
		"cache_gets_total result=hit strategy=volatile":                        2,
		"cache_gets_total result=miss strategy=volatile":                       1,
		"cache_operations_total operation=get outcome=ok strategy=volatile":    3,
		"cache_operations_total operation=get outcome=error strategy=volatile": 1,
	} {
		if got := m.counts[key]; got != want {
			t.Errorf("%s = %v, want %v (all: %v)", key, got, want, m.counts)
		}
	}
}
