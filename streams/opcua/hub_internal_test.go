package opcua

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
)

func TestTheFirstReportIsSkippedOnlyWhenItIsTheValueRead(t *testing.T) {
	at := time.Now()
	read := &ua.DataValue{Value: ua.MustVariant(int32(1)), SourceTimestamp: at}

	// A later write that replaced the initial value in a full queue.
	r := &route{initial: initial{read: read}}
	if !r.first(&ua.DataValue{Value: ua.MustVariant(int32(2)), SourceTimestamp: time.Now(), ServerTimestamp: time.Now()}) {
		t.Error("a later write was skipped")
	}

	// The value read, reported with a server timestamp of its own.
	r = &route{initial: initial{read: read}}
	reported := *read
	reported.ServerTimestamp = time.Now()
	if r.first(&reported) {
		t.Error("the initial value was delivered")
	}
	if !r.first(&reported) {
		t.Error("a second report was skipped too")
	}

	// A node that could not be read first: its first report is skipped.
	r = &route{}
	if r.first(read) || !r.first(read) {
		t.Error("an unread node's first report was not the only one skipped")
	}
}

func TestWriteStatusesMapToSettledErrors(t *testing.T) {
	for _, status := range []ua.StatusCode{ua.StatusOK, ua.StatusUncertainLastUsableValue} {
		if err := writeStatus("s", status); err != nil {
			t.Errorf("%v: %v", status, err)
		}
	}
	err := writeStatus("ns=2;s=x", ua.StatusBadTypeMismatch)
	if !errors.Is(err, ua.StatusBadTypeMismatch) || !strings.Contains(err.Error(), "ns=2;s=x") || errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("BadTypeMismatch = %v", err)
	}
	for _, status := range []ua.StatusCode{ua.StatusBadNotWritable, ua.StatusBadUserAccessDenied, ua.StatusBadNodeIDUnknown} {
		if err := writeStatus("s", status); !errors.Is(err, streams.ErrUnsupported) || !errors.Is(err, status) {
			t.Errorf("%v = %v, want ErrUnsupported", status, err)
		}
	}
	if err := writeStatus("s", ua.StatusBadTimeout); err == nil || errors.Is(err, streams.ErrUnsupported) {
		t.Errorf("BadTimeout = %v, want a retryable error", err)
	}
}

func TestWhatAWriteCannotDoIsRefused(t *testing.T) {
	for _, tc := range []struct {
		opt  streams.Option
		says string
	}{
		{streams.TTL(time.Second), "schedule"},
		{streams.Expires(time.Second), "expire"},
		{streams.Retain(), "retain"},
		{streams.Header("lang", "go"), "headers"},
	} {
		err := checkPublish(streams.Stream{}, streams.NewOptions(tc.opt))
		if !errors.Is(err, streams.ErrUnsupported) || !strings.Contains(err.Error(), tc.says) {
			t.Errorf("%s: %v", tc.says, err)
		}
	}
	if err := checkPublish(streams.Stream{}, streams.NewOptions(streams.Expires(0))); err != nil {
		t.Errorf("a zero expiry: %v", err)
	}
}

func TestSubjectsParseInEveryForm(t *testing.T) {
	n, u, err := parseSubject("ns=3;s=Temperature")
	if err != nil || u != nil || n.Namespace() != 3 || n.StringID() != "Temperature" {
		t.Errorf("ns=3;s=Temperature = %v %v %v", n, u, err)
	}
	if n, _, perr := parseSubject("i=2258"); perr != nil || n.IntID() != 2258 {
		t.Errorf("i=2258 = %v %v", n, perr)
	}
	_, u, err = parseSubject("nsu=urn:plant%3bline;i=1001")
	if err != nil || u == nil || u.uri != "urn:plant;line" || u.id != "i=1001" {
		t.Errorf("nsu = %+v %v", u, err)
	}
	if got, err := u.node(4); err != nil || got.Namespace() != 4 || got.IntID() != 1001 {
		t.Errorf("node(4) = %v %v", got, err)
	}
}

// A DataValue is written as given, every field it sets marked on the wire.
func TestADataValueIsWrittenAsGiven(t *testing.T) {
	at := time.Date(2026, 3, 14, 15, 9, 26, 0, time.UTC)
	dv, err := dataValue(nil, "id", &ua.DataValue{
		Value: ua.MustVariant(19.5), Status: ua.StatusUncertainLastUsableValue, SourceTimestamp: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := byte(ua.DataValueValue | ua.DataValueStatusCode | ua.DataValueSourceTimestamp)
	if dv.EncodingMask != want || !dv.SourceTimestamp.Equal(at) || dv.Status != ua.StatusUncertainLastUsableValue {
		t.Errorf("dataValue = %+v", dv)
	}
	plain, err := dataValue(streams.JSON, "id", 19.5)
	if err != nil || plain.EncodingMask != ua.DataValueValue || !plain.SourceTimestamp.IsZero() {
		t.Errorf("a plain value = %+v, %v", plain, err)
	}
}
