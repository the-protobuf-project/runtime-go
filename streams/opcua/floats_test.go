package opcua

import (
	"math"
	"testing"

	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
)

// same is equality that counts NaN as equal to itself.
func same(a, b float64) bool { return a == b || (math.IsNaN(a) && math.IsNaN(b)) }

// A good reading of NaN or an infinity is delivered, and decodes back into the
// float type it was.
func TestDecodeCarriesNonFiniteFloats(t *testing.T) {
	for _, want := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		msg, err := decode(streams.NewRegistry(), "s", &ua.DataValue{Value: ua.MustVariant(want)})
		if err != nil {
			t.Fatalf("decode(%v): %v", want, err)
		}
		var got float64
		if err := msg.Decode(&got); err != nil || !same(got, want) {
			t.Fatalf("float64 %v came back as %v, %v", want, got, err)
		}

		msg, err = decode(streams.NewRegistry(), "s", &ua.DataValue{Value: ua.MustVariant(float32(want))})
		if err != nil {
			t.Fatalf("decode(float32 %v): %v", want, err)
		}
		var got32 float32
		if err := msg.Decode(&got32); err != nil || !same(float64(got32), want) {
			t.Fatalf("float32 %v came back as %v, %v", want, got32, err)
		}
	}
}

// A slice with a non-finite float in it comes back whole, ordinary values
// included.
func TestDecodeCarriesNonFiniteFloatSlices(t *testing.T) {
	want := []float64{1.5, math.NaN(), math.Inf(1), -2}
	msg, err := decode(streams.NewRegistry(), "s", &ua.DataValue{Value: ua.MustVariant(want)})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	var got []float64
	if err := msg.Decode(&got); err != nil || len(got) != len(want) {
		t.Fatalf("got %v, %v; want %v", got, err, want)
	}
	for i := range want {
		if !same(got[i], want[i]) {
			t.Fatalf("entry %d = %v, want %v", i, got[i], want[i])
		}
	}

	msg, err = decode(streams.NewRegistry(), "s", &ua.DataValue{Value: ua.MustVariant([]float32{0.25, float32(math.Inf(-1))})})
	if err != nil {
		t.Fatalf("decode float32s: %v", err)
	}
	var got32 []float32
	if err := msg.Decode(&got32); err != nil || len(got32) != 2 || got32[0] != 0.25 || !math.IsInf(float64(got32[1]), -1) {
		t.Fatalf("float32s came back as %v, %v", got32, err)
	}
}

// A finite float still goes out as plain JSON.
func TestDecodeKeepsFiniteFloatsAsJSON(t *testing.T) {
	msg, err := decode(streams.NewRegistry(), "s", &ua.DataValue{Value: ua.MustVariant(1.5)})
	if err != nil || string(msg.Data) != "1.5" {
		t.Fatalf("data = %q, %v; want 1.5", msg.Data, err)
	}
}
