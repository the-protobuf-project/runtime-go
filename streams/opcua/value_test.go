package opcua

import (
	"errors"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
)

func TestEncodeWritesOPCUATypesAsThemselves(t *testing.T) {
	cases := map[ua.TypeID]any{
		ua.TypeIDBoolean:  true,
		ua.TypeIDInt32:    int32(7),
		ua.TypeIDDouble:   1.5,
		ua.TypeIDString:   "on",
		ua.TypeIDDateTime: time.Unix(0, 0),
	}
	for want, v := range cases {
		got, err := encode(streams.JSON, "id", v)
		if err != nil {
			t.Fatalf("encode(%T): %v", v, err)
		}
		if got.Type() != want {
			t.Fatalf("encode(%T) wrote %v, want %v", v, got.Type(), want)
		}
	}
}

func TestEncodeFramesEverythingElse(t *testing.T) {
	v, err := encode(streams.JSON, "id-1", struct{ A int }{A: 1})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if v.Type() != ua.TypeIDByteString {
		t.Fatalf("a struct was written as %v, want a ByteString", v.Type())
	}

	msg, err := decode(streams.NewRegistry(), "s", &ua.DataValue{Value: v})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	var got struct{ A int }
	if err := msg.Decode(&got); err != nil || got.A != 1 || msg.ID != "id-1" {
		t.Fatalf("round trip = %+v id %q, %v", got, msg.ID, err)
	}
}

func TestEncodeRefusesGoInt(t *testing.T) {
	for _, v := range []any{1, uint(1), []int{1}} {
		if _, err := encode(streams.JSON, "id", v); !errors.Is(err, streams.ErrUnsupported) {
			t.Fatalf("encode(%T) = %v, want ErrUnsupported", v, err)
		}
	}
}

func TestDecodeRefusesWhatIsNotAReading(t *testing.T) {
	cases := map[string]*ua.DataValue{
		"nil":       nil,
		"bad":       {Status: ua.StatusBadNodeIDUnknown, Value: ua.MustVariant(1.0)},
		"uncertain": {Status: ua.StatusUncertain, Value: ua.MustVariant(1.0)},
		"empty":     {},
	}
	for name, dv := range cases {
		if _, err := decode(streams.NewRegistry(), "s", dv); err == nil {
			t.Fatalf("%s: decode delivered it", name)
		}
	}
}

func TestDecodeCarriesForeignBytesAsThemselves(t *testing.T) {
	msg, err := decode(streams.NewRegistry(), "s", &ua.DataValue{Value: ua.MustVariant([]byte("raw"))})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	var got []byte
	if err := msg.Decode(&got); err != nil || string(got) != "raw" {
		t.Fatalf("got %q, %v; want raw", got, err)
	}
}

func TestSampleIDPrefersTheSourceTimestamp(t *testing.T) {
	src := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	srv := src.Add(time.Second)

	if got := sampleID(&ua.DataValue{SourceTimestamp: src, ServerTimestamp: srv}); got != src.Format(time.RFC3339Nano) {
		t.Fatalf("id = %q, want the source timestamp", got)
	}
	if got := sampleID(&ua.DataValue{ServerTimestamp: srv}); got != srv.Format(time.RFC3339Nano) {
		t.Fatalf("id = %q, want the server timestamp", got)
	}
	if got := sampleID(&ua.DataValue{}); got != "" {
		t.Fatalf("id = %q, want empty with no timestamp", got)
	}
}
