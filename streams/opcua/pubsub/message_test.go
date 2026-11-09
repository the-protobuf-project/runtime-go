package pubsub

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/the-protobuf-project/runtime-go/streams"
)

// testdata/runtime-rs.txt was printed by a small program built on runtime-rs's
// streams crate (opcua feature): the NetworkMessage and MetaDataMessage it
// writes for the values below, each value's OPC UA binary encoding, what its
// reader makes of its own JSON and of foreign forms, a DataSet's binary
// payload, and DataSetWriter ids. Matching it byte for byte is what makes the
// two providers read each other.
type rustFixture struct {
	network  string
	metadata string
	dataset  string
	binary   map[string]string
	decoded  map[string]string
	writerID map[string]string
	foreign  [][2]string
}

func loadRust(t *testing.T) rustFixture {
	t.Helper()
	f, err := os.Open("testdata/runtime-rs.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fx := rustFixture{binary: map[string]string{}, decoded: map[string]string{}, writerID: map[string]string{}}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		kind, rest, _ := strings.Cut(sc.Text(), " ")
		switch kind {
		case "NETWORK":
			fx.network = rest
		case "METADATA":
			fx.metadata = rest
		case "DATASET":
			fx.dataset = rest
		case "BINARY", "DECODED", "WRITERID":
			name, value, _ := strings.Cut(rest, " ")
			map[string]map[string]string{"BINARY": fx.binary, "DECODED": fx.decoded, "WRITERID": fx.writerID}[kind][name] = value
		case "FOREIGN":
			raw, out, _ := strings.Cut(rest, " => ")
			fx.foreign = append(fx.foreign, [2]string{raw, out})
		}
	}
	return fx
}

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func value(t *testing.T, v any) *ua.DataValue {
	t.Helper()
	dv, err := DataValueOf(v)
	if err != nil {
		t.Fatalf("%v: %v", v, err)
	}
	return dv
}

func u32(n uint32) *uint32 { return &n }

// rustFields are the values the fixture program wrote, in its order.
func rustFields(t *testing.T) []Field {
	at := mustTime(t, "2026-11-09T10:00:00.1234567Z")
	whole := mustTime(t, "2026-11-09T10:00:00Z")
	return []Field{
		{"Bool", value(t, true)},
		{"SByte", value(t, int8(-5))},
		{"Byte", value(t, uint8(200))},
		{"Int16", value(t, int16(-300))},
		{"UInt16", value(t, uint16(60000))},
		{"Int32", value(t, int32(-3))},
		{"UInt32", value(t, uint32(4000000000))},
		{"Int64", value(t, int64(-9007199254740993))},
		{"UInt64", value(t, uint64(18446744073709551615))},
		{"Float", value(t, float32(1.5))},
		{"FloatTenth", value(t, float32(0.1))},
		{"FloatNaN", value(t, math.Float32frombits(0x7fc00000))},
		{"DoubleInf", value(t, math.Inf(1))},
		{"DoubleNegInf", value(t, math.Inf(-1))},
		{"Double", value(t, 21.5)},
		{"DoubleWhole", value(t, 3.0)},
		{"DoubleSmall", value(t, 1e-7)},
		{"DoubleBig", value(t, 1e21)},
		{"String", value(t, "héllo \"q\" \n</>")},
		{"DateTime", value(t, at)},
		{"Guid", value(t, ua.NewGUID("72962b91-fa75-4ae6-8d28-b404dc7daf63"))},
		{"ByteString", value(t, []byte{0, 1, 2, 250})},
		{"StatusCode", value(t, ua.StatusBadSensorFailure)},
		{"QualifiedName", value(t, &ua.QualifiedName{NamespaceIndex: 2, Name: "Temp"})},
		{"LocalizedText", value(t, ua.NewLocalizedTextWithLocale("Hot", "en"))},
		{"NodeIdNum", value(t, numericNodeID(0, 2258))},
		{"NodeIdStr", value(t, ua.NewStringNodeID(2, "Line1.Temp"))},
		{"Int32Array", value(t, []int32{1, 2, 3})},
		{"DoubleArray", value(t, []float64{1.5, 2})},
		{"StringArray", value(t, []string{"a", "b"})},
		{"Empty", &ua.DataValue{}},
		{"Full", &ua.DataValue{
			Value: ua.MustVariant(7.25), Status: ua.StatusUncertainLastUsableValue,
			SourceTimestamp: at, SourcePicoseconds: 12, ServerTimestamp: whole, ServerPicoseconds: 34,
		}},
		{"StatusOnly", &ua.DataValue{Status: ua.StatusBadSensorFailure}},
	}
}

func encode(t *testing.T, dv *ua.DataValue) string {
	t.Helper()
	b, err := encodeDataValue(dv)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func TestWritesWhatRuntimeRsWrites(t *testing.T) {
	fx := loadRust(t)
	fields := rustFields(t)
	network := &NetworkMessage{
		MessageID: "01JMSG", PublisherID: "plant-1", WriterGroupName: "line-a",
		Messages: []DataSetMessage{
			{
				DataSetWriterID: WriterID("boiler"), DataSetWriterName: "boiler", SequenceNumber: u32(41),
				Timestamp:       mustTime(t, "2026-11-09T10:00:00.1234567Z"),
				MetaDataVersion: &ua.ConfigurationVersionDataType{MajorVersion: 3, MinorVersion: 4},
				Payload:         fields,
			},
			{
				DataSetWriterID: 7, DataSetWriterName: "pump", SequenceNumber: u32(42),
				Timestamp: mustTime(t, "2026-11-09T10:00:00Z"),
				Payload:   []Field{{"Count", value(t, int32(-3))}},
			},
		},
	}
	got, err := network.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != fx.network {
		t.Fatalf("NetworkMessage differs from runtime-rs's\n go: %s\nrust: %s", got, fx.network)
	}
	for _, f := range fields {
		if f.Name == "FloatNaN" {
			nanBits(t, encode(t, f.Value))
			continue
		}
		if want := fx.binary[f.Name]; encode(t, f.Value) != want {
			t.Errorf("%s: binary %s, runtime-rs %s", f.Name, encode(t, f.Value), want)
		}
	}
}

func TestReadsWhatRuntimeRsWrites(t *testing.T) {
	fx := loadRust(t)
	network, err := ParseNetworkMessage([]byte(fx.network))
	if err != nil {
		t.Fatal(err)
	}
	if len(network.Messages) != 2 || len(network.Messages[0].Payload) != len(fx.decoded) {
		t.Fatalf("read %d messages", len(network.Messages))
	}
	for _, f := range network.Messages[0].Payload {
		if f.Name == "FloatNaN" {
			nanBits(t, encode(t, f.Value))
			continue
		}
		want := fx.decoded[f.Name]
		if f.Name == "QualifiedName" {
			// runtime-rs reads "2:Temp" as a name in namespace 0 because its
			// reading context knows no namespace 2; this reader keeps the
			// index, which is the value that was written.
			want = fx.binary[f.Name]
		}
		if got := encode(t, f.Value); got != want {
			t.Errorf("%s: read as %s, runtime-rs reads %s", f.Name, got, want)
		}
	}
	// And reading it back writes it again unchanged, but for the DateTime
	// Variant, which Part 6 JSON carries to the millisecond.
	again, err := network.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != fx.network {
		t.Fatalf("re-encoded differently\n got: %s\nwant: %s", again, fx.network)
	}
}

// nanBits checks a NaN Float's binary. gopcua writes every NaN as Part 6's
// quiet NaN, 0xFFC00000, where runtime-rs writes Rust's f32::NAN,
// 0x7FC00000; both read as NaN everywhere, so this is the one value whose
// bytes differ between the two providers.
func nanBits(t *testing.T, got string) {
	t.Helper()
	if got != "010a0000c0ff" {
		t.Errorf("NaN: %s", got)
	}
}

func TestReadsForeignFormsAsRuntimeRsDoes(t *testing.T) {
	for _, c := range loadRust(t).foreign {
		raw, want := c[0], c[1]
		dv, err := fieldValue([]byte(raw))
		if strings.HasPrefix(want, "ERR") {
			if err == nil {
				t.Errorf("%s: read, runtime-rs refuses it", raw)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", raw, err)
			continue
		}
		if got := encode(t, dv); got != want {
			t.Errorf("%s: %s, runtime-rs %s", raw, got, want)
		}
	}
}

func TestMetadataIsWhatRuntimeRsWrites(t *testing.T) {
	fx := loadRust(t)
	set := NewDataSet().
		With("Temperature", 21.5).
		With("Samples", []int32{1, 2}).
		With("Nothing", &ua.DataValue{}).
		With("Name", "x")
	meta := set.Metadata("boiler")
	meta.ConfigurationVersion = &ua.ConfigurationVersionDataType{MajorVersion: 845000000, MinorVersion: 845000001}
	message := &MetaDataMessage{
		MessageID: "m-1", PublisherID: "plant-1", DataSetWriterID: WriterID("boiler"),
		WriterGroupName: "line-a", DataSetWriterName: "boiler",
		Timestamp: mustTime(t, "2026-11-09T10:00:00.1234567Z"), MetaData: meta,
	}
	got, err := message.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != fx.metadata {
		t.Fatalf("metadata differs from runtime-rs's\n go: %s\nrust: %s", got, fx.metadata)
	}
	b, err := set.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(b) != fx.dataset {
		t.Fatalf("DataSet binary %x, runtime-rs %s", b, fx.dataset)
	}
	read, err := ParseMetaDataMessage([]byte(fx.metadata))
	if err != nil {
		t.Fatal(err)
	}
	if *read.MetaData.ConfigurationVersion != *meta.ConfigurationVersion || len(read.MetaData.Fields) != 4 ||
		read.MetaData.Fields[1].ValueRank != 1 || read.MetaData.Fields[2].DataType.IntID() != 24 {
		t.Fatalf("read back %+v", read.MetaData)
	}
}

func TestWriterIDsMatchRuntimeRs(t *testing.T) {
	for name, want := range loadRust(t).writerID {
		if got := WriterID(name); want != itoa(got) {
			t.Errorf("WriterID(%q) = %d, runtime-rs %s", name, got, want)
		}
	}
}

func itoa(n uint16) string { b, _ := json.Marshal(n); return string(b) }

// sample is a NetworkMessage with two DataSetMessages, for the encode and
// decode tests.
func sample(t *testing.T) *NetworkMessage {
	at := mustTime(t, "2026-11-09T10:00:00Z")
	uncertain := ua.StatusUncertainLastUsableValue
	return &NetworkMessage{
		MessageID: "01JMSG", PublisherID: "plant-1", WriterGroupName: "line-a",
		Messages: []DataSetMessage{
			{
				DataSetWriterID: 7, DataSetWriterName: "boiler", SequenceNumber: u32(41), Timestamp: at,
				MetaDataVersion: &ua.ConfigurationVersionDataType{MajorVersion: 3, MinorVersion: 4},
				Payload: []Field{
					{"Temperature", value(t, 21.5)},
					{"Running", &ua.DataValue{
						EncodingMask: ua.DataValueValue | ua.DataValueStatusCode | ua.DataValueSourceTimestamp,
						Value:        ua.MustVariant(true), Status: uncertain, SourceTimestamp: at,
					}},
				},
			},
			{
				DataSetWriterID: 7, DataSetWriterName: "boiler", SequenceNumber: u32(42),
				Payload: []Field{{"Count", value(t, int32(-3))}},
			},
		},
	}
}

func TestRoundTripsABatch(t *testing.T) {
	message := sample(t)
	b, err := message.JSON()
	if err != nil {
		t.Fatal(err)
	}
	back, err := ParseNetworkMessage(b)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := back.JSON()
	if !bytes.Equal(again, b) {
		t.Fatalf("round trip changed it\n got: %s\nwant: %s", again, b)
	}
	if *back.Messages[1].SequenceNumber != 42 || back.Messages[0].Payload[1].Value.Status != ua.StatusUncertainLastUsableValue {
		t.Fatalf("read back %+v", back)
	}
}

func TestWritesPart14FieldNames(t *testing.T) {
	b, _ := sample(t).JSON()
	var j map[string]any
	if err := json.Unmarshal(b, &j); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]any{"MessageType": "ua-data", "MessageId": "01JMSG", "PublisherId": "plant-1", "WriterGroupName": "line-a"} {
		if j[k] != want {
			t.Errorf("%s = %v", k, j[k])
		}
	}
	if _, ok := j["DataSetClassId"]; ok {
		t.Error("absent fields are omitted")
	}
	first := j["Messages"].([]any)[0].(map[string]any)
	if first["DataSetWriterId"] != 7.0 || first["SequenceNumber"] != 41.0 || first["Timestamp"] != "2026-11-09T10:00:00Z" {
		t.Errorf("first message %v", first)
	}
	temp, _ := json.Marshal(first["Payload"].(map[string]any)["Temperature"])
	if string(temp) != `{"Value":{"Body":21.5,"Type":11}}` {
		t.Errorf("Temperature %s", temp)
	}
}

func TestReadsForeignFieldForms(t *testing.T) {
	cases := []struct {
		raw  string
		want any
	}{
		{`{"Type":10,"Body":1.5}`, float32(1.5)},
		{`{"UaType":7,"Value":12}`, uint32(12)},
		{`{"Value":21,"SourceTimestamp":"2026-01-01T00:00:00Z"}`, int64(21)},
		{"3.25", 3.25},
		{`"on"`, "on"},
		{"true", true},
		{"[1,2]", []int64{1, 2}},
	}
	for _, c := range cases {
		dv, err := fieldValue([]byte(c.raw))
		if err != nil {
			t.Fatalf("%s: %v", c.raw, err)
		}
		if got := dv.Value.Value(); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: %#v, want %#v", c.raw, got, c.want)
		}
	}
	dv, err := fieldValue([]byte(`{"Value":1,"StatusCode":{"Code":2149842944}}`))
	if err != nil || dv.Status != ua.StatusCode(2149842944) {
		t.Fatalf("%v %v", dv, err)
	}
	if _, err := fieldValue([]byte(`{"x":1}`)); err == nil {
		t.Fatal("a structure is not a DataValue")
	}
}

func TestReadsAMessageWithoutItsHeader(t *testing.T) {
	m, err := ParseNetworkMessage([]byte(`{"DataSetWriterId":3,"SequenceNumber":9,"Payload":{"a":{"Type":6,"Body":1}}}`))
	if err != nil || len(m.Messages) != 1 || *m.Messages[0].SequenceNumber != 9 || m.Messages[0].DataSetWriterID != 3 {
		t.Fatalf("%+v %v", m, err)
	}
	m, err = ParseNetworkMessage([]byte(`[{"SequenceNumber":1,"Payload":{}},{"SequenceNumber":2,"Payload":{}}]`))
	if err != nil || len(m.Messages) != 2 {
		t.Fatalf("%+v %v", m, err)
	}
	// A single DataSetMessage object under Messages, and a numeric
	// PublisherId.
	m, err = ParseNetworkMessage([]byte(`{"MessageType":"ua-data","PublisherId":12,"Messages":{"Payload":{}}}`))
	if err != nil || m.PublisherID != "12" || len(m.Messages) != 1 {
		t.Fatalf("%+v %v", m, err)
	}
}

func TestRefusesWhatIsNotData(t *testing.T) {
	for _, raw := range []string{
		`{"MessageType":"ua-metadata","MetaData":{}}`,
		"42",
		`{"Messages":[]} trailing`,
		`{"Messages":[{"Payload":[1]}]}`,
	} {
		if _, err := ParseNetworkMessage([]byte(raw)); err == nil {
			t.Errorf("%s: read", raw)
		}
	}
}

// Captured from Microsoft OPC Publisher 2.9.15 (--strict --mm=PubSub
// --me=JsonReversible) reading opc-plc, as docker/compose.opcua.pubsub.yaml
// runs it.
const (
	opcPublisherData     = `{"MessageId":"9c484127-94a8-4977-8293-6f464b4460da","MessageType":"ua-data","PublisherId":"opc-publisher","DataSetWriterGroup":"plc","Messages":[{"SequenceNumber":49,"MetaDataVersion":{"MajorVersion":1,"MinorVersion":1143887952},"Timestamp":"2026-11-09T05:14:25.8619034Z","MessageType":"ua-deltaframe","DataSetWriterName":"simulation","Payload":{"StepUp":{"Value":{"Type":7,"Body":275},"SourceTimestamp":"2026-11-09T05:14:25.798419Z","ServerTimestamp":"2026-11-09T05:14:25.7984391Z"},"RandomSignedInt32":{"Value":{"Type":6,"Body":-670934554},"SourceTimestamp":"2026-11-09T05:14:25.7984578Z","ServerTimestamp":"2026-11-09T05:14:25.7984703Z"}}}]}`
	opcPublisherMetadata = `{"MessageId":"40e1afec-1bbd-4fa4-8765-db9d4e943b3b","MessageType":"ua-metadata","PublisherId":"opc-publisher","DataSetWriterGroup":"plc","MetaData":{"Namespaces":["http://opcfoundation.org/UA/","urn:OpcPlc:opc-plc","http://opcfoundation.org/UA/DI/","http://microsoft.com/Opc/OpcPlc/","http://microsoft.com/Opc/OpcPlc/Boiler","http://microsoft.com/Opc/OpcPlc/BoilerInstance","http://microsoft.com/Opc/OpcPlc/ReferenceTest","http://opcfoundation.org/UA/Diagnostics"],"StructureDataTypes":[],"EnumDataTypes":[],"SimpleDataTypes":[],"Fields":[{"Name":"StepUp","Description":"Constantly increasing value","BuiltInType":7,"DataType":{"Id":7},"ValueRank":-1,"ArrayDimensions":[],"DataSetFieldId":"2334228f-4e90-46e5-99c4-ccba6ab0bf88","Properties":[]},{"Name":"AlternatingBoolean","Description":"Alternating boolean value","BuiltInType":1,"DataType":{"Id":1},"ValueRank":-1,"ArrayDimensions":[],"DataSetFieldId":"cb6461e9-4df5-4c4a-ac2f-6919b8514f4e","Properties":[]},{"Name":"RandomSignedInt32","Description":"Random signed 32 bit integer value","BuiltInType":6,"DataType":{"Id":6},"ValueRank":-1,"ArrayDimensions":[],"DataSetFieldId":"c36fbf3f-393e-4258-8b0d-4909cf4fbf92","Properties":[]}],"ConfigurationVersion":{"MajorVersion":1,"MinorVersion":1143887952}},"DataSetWriterName":"simulation"}`
)

func TestReadsOPCPublisher(t *testing.T) {
	network, err := ParseNetworkMessage([]byte(opcPublisherData))
	if err != nil {
		t.Fatal(err)
	}
	if network.PublisherID != "opc-publisher" || network.WriterGroupName != "plc" {
		t.Fatalf("%+v", network)
	}
	m := network.Messages[0]
	if m.DataSetWriterName != "simulation" || *m.SequenceNumber != 49 || m.MessageType != "ua-deltaframe" || m.MetaDataVersion.MajorVersion != 1 {
		t.Fatalf("%+v", m)
	}
	if m.Payload[0].Name != "StepUp" || m.Payload[0].Value.Value.Value() != uint32(275) {
		t.Fatalf("%+v", m.Payload[0])
	}
	// Seven decimal places of a .NET DateTime survive.
	if ns := m.Payload[0].Value.ServerTimestamp.Nanosecond(); ns != 798439100 {
		t.Fatalf("server timestamp ns %d", ns)
	}
	if m.Payload[1].Value.Value.Value() != int32(-670934554) {
		t.Fatalf("%+v", m.Payload[1])
	}

	meta, err := ParseMetaDataMessage([]byte(opcPublisherMetadata))
	if err != nil {
		t.Fatal(err)
	}
	if meta.WriterGroupName != "plc" || meta.DataSetWriterName != "simulation" ||
		meta.MetaData.Fields[0].Name != "StepUp" || meta.MetaData.Fields[0].BuiltInType != 7 ||
		meta.MetaData.Fields[0].Description.Text != "Constantly increasing value" {
		t.Fatalf("%+v", meta)
	}
}

func TestRoundTripsMetadata(t *testing.T) {
	message := &MetaDataMessage{
		MessageID: "m", PublisherID: "plant-1", DataSetWriterID: 7, WriterGroupName: "line-a", DataSetWriterName: "boiler",
		MetaData: ua.DataSetMetaDataType{
			Name:                 "boiler",
			Fields:               []*ua.FieldMetaData{{Name: "Temperature", BuiltInType: 11, ValueRank: -1}},
			ConfigurationVersion: &ua.ConfigurationVersionDataType{MajorVersion: 1, MinorVersion: 2},
		},
	}
	b, err := message.JSON()
	if err != nil {
		t.Fatal(err)
	}
	var j map[string]any
	_ = json.Unmarshal(b, &j)
	if j["MessageType"] != "ua-metadata" || j["MetaData"].(map[string]any)["Fields"].([]any)[0].(map[string]any)["Name"] != "Temperature" {
		t.Fatalf("%s", b)
	}
	back, err := ParseMetaDataMessage(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, *message) {
		t.Fatalf("round trip\n got %+v\nwant %+v", back, *message)
	}
	if _, err := ParseNetworkMessage(b); err == nil {
		t.Fatal("metadata is not data")
	}
}

func TestDataSetRoundTrips(t *testing.T) {
	set := NewDataSet().
		With("Temperature", 21.5).
		With("Mode", &ua.DataValue{Value: ua.MustVariant("auto"), SourceTimestamp: time.Now().UTC().Truncate(100)})
	b, err := set.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var back DataSet
	if err := back.UnmarshalBinary(b); err != nil {
		t.Fatal(err)
	}
	again, _ := back.MarshalBinary()
	if !bytes.Equal(again, b) || back.Fields[1].Name != "Mode" {
		t.Fatalf("round trip changed it")
	}
}

func TestDataSetDescribesItsFields(t *testing.T) {
	meta := NewDataSet().
		With("Temperature", 21.5).
		With("Samples", []int32{1, 2}).
		With("Nothing", &ua.DataValue{}).
		Metadata("boiler")
	if meta.Name != "boiler" {
		t.Fatal(meta.Name)
	}
	f := meta.Fields
	if f[0].BuiltInType != 11 || f[0].ValueRank != -1 || f[0].DataType.IntID() != 11 {
		t.Errorf("%+v", f[0])
	}
	if f[1].BuiltInType != 6 || f[1].ValueRank != 1 {
		t.Errorf("%+v", f[1])
	}
	if f[2].DataType.IntID() != 24 || f[2].ValueRank != -2 {
		t.Errorf("%+v", f[2])
	}
}

func TestDataSetRefusesOtherValuesAndGarbage(t *testing.T) {
	if err := NewDataSet().With("x", struct{}{}).Err(); !errors.Is(err, streams.ErrUnsupported) {
		t.Fatalf("a struct is not an OPC UA value: %v", err)
	}
	if err := NewDataSet().With("x", 1).Err(); !errors.Is(err, streams.ErrUnsupported) {
		t.Fatalf("Go's int has no OPC UA type: %v", err)
	}
	for _, b := range [][]byte{{}, {1, 0, 0, 0}, {0xff, 0xff, 0xff, 0xff}, {0, 0, 0, 0, 9}} {
		var d DataSet
		if err := d.UnmarshalBinary(b); err == nil {
			t.Errorf("%x: read", b)
		}
	}
}

func TestTheCodecDecodesIntoGoTypes(t *testing.T) {
	data, err := DataValueCodec.Marshal(int32(42))
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(data) != "01062a000000" {
		t.Fatalf("not the OPC UA binary encoding: %x", data)
	}
	msg := streams.NewMessage("id", "w/f", data, DataValueCodec)
	var n int32
	var dv ua.DataValue
	var v any
	if err := msg.Decode(&n); err != nil || n != 42 {
		t.Fatalf("%d %v", n, err)
	}
	if err := msg.Decode(&dv); err != nil || dv.Value.Value() != int32(42) {
		t.Fatalf("%v %v", dv, err)
	}
	if err := msg.Decode(&v); err != nil || v != int32(42) {
		t.Fatalf("%v %v", v, err)
	}
	var s string
	if err := msg.Decode(&s); err == nil {
		t.Fatal("an Int32 is not a string")
	}
	if _, err := decodeDataValue(append(data, 0)); err == nil || !strings.Contains(err.Error(), "follow") {
		t.Fatalf("trailing bytes: %v", err)
	}
	if _, err := decodeDataValue([]byte{0x01, 0x06, 0x01}); err == nil {
		t.Fatal("truncated")
	}
}

func TestWriterIDsAreStableAndNeverZero(t *testing.T) {
	if WriterID("boiler") != WriterID(strings.Clone("boiler")) || WriterID("boiler") == WriterID("pump") {
		t.Fatal("unstable")
	}
	for n := 0; n < 5000; n++ {
		if WriterID("w"+itoa(uint16(n))) == 0 {
			t.Fatal("zero")
		}
	}
}

func TestDeclarationsCheckTopicLevels(t *testing.T) {
	ok := []struct {
		id       string
		subjects []string
	}{{"line-a", []string{"boiler", "pump/Speed"}}, {"plant-2/line-a", []string{"boiler"}}}
	for _, c := range ok {
		if _, err := declaration(c.id, streams.Stream{Subjects: c.subjects}); err != nil {
			t.Errorf("%s %v: %v", c.id, c.subjects, err)
		}
	}
	bad := []struct {
		id      string
		subject string
	}{{"a/b/c", "w"}, {"", "w"}, {"line+", "w"}, {"line", "w+"}, {"line", "#"}, {"line", "w/"}, {"line", "/f"}, {"line", "w/f#"}}
	for _, c := range bad {
		if _, err := declaration(c.id, streams.Stream{Subjects: []string{c.subject}}); !errors.Is(err, streams.ErrUnsupported) {
			t.Errorf("%s %s: %v", c.id, c.subject, err)
		}
	}
}

func TestVersionTimeCountsFrom2000(t *testing.T) {
	// 2026 is about 26 years of seconds past 2000.
	if versionTime(time.Now()) <= 26*365*24*3600 {
		t.Fatal(versionTime(time.Now()))
	}
}

func TestSequenceSteps(t *testing.T) {
	cases := []struct {
		last   *uint32
		seq    uint32
		step   step
		missed uint32
	}{
		{nil, 7, inOrder, 0}, {u32(7), 8, inOrder, 0}, {u32(7), 10, gap, 2}, {u32(7), 7, repeat, 0},
		{u32(700), 1, restart, 0},
		// UInt32 wraps.
		{u32(math.MaxUint32), 0, inOrder, 0}, {u32(math.MaxUint32), 2, gap, 2},
	}
	for _, c := range cases {
		if s, m := stepOf(c.last, c.seq); s != c.step || m != c.missed {
			t.Errorf("%v %d: %v %d", c.last, c.seq, s, m)
		}
	}
}

func TestSubjectsSplitAtTheFirstSlash(t *testing.T) {
	for subject, want := range map[string][2]string{"boiler": {"boiler", ""}, "boiler/Temp": {"boiler", "Temp"}, "boiler/a/b": {"boiler", "a/b"}} {
		if w, f, _ := split(subject); w != want[0] || f != want[1] {
			t.Errorf("%s: %s %s", subject, w, f)
		}
	}
}
