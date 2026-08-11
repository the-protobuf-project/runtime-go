package nats

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestConvertAnyDataToBytes_String(t *testing.T) {
	in := "hello"
	got, err := ConvertAnyDataToBytes(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("want %q, got %q", "hello", string(got))
	}
}

func TestConvertAnyDataToBytes_Bytes(t *testing.T) {
	in := []byte{0x01, 0x02, 0x03}
	got, err := ConvertAnyDataToBytes(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(got, in) {
		t.Fatalf("want %v, got %v", in, got)
	}
}

func TestConvertAnyDataToBytes_StructJSON(t *testing.T) {
	type person struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	in := person{Name: "Ava", Age: 7}

	got, err := ConvertAnyDataToBytes(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(got, &m); err != nil {
		t.Fatalf("unmarshal produced json failed: %v", err)
	}
	if m["name"] != "Ava" || int(m["age"].(float64)) != 7 {
		t.Fatalf("unexpected json content: %v", m)
	}
}

func TestConvertAnyDataToBytes_MapJSON(t *testing.T) {
	in := map[string]any{"ok": true, "n": 42}
	got, err := ConvertAnyDataToBytes(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(got, &m); err != nil {
		t.Fatalf("unmarshal produced json failed: %v", err)
	}
	if m["ok"] != true || int(m["n"].(float64)) != 42 {
		t.Fatalf("unexpected json content: %v", m)
	}
}

func TestConvertAnyDataToBytes_NilBecomesJSONNull(t *testing.T) {
	var in any = nil
	got, err := ConvertAnyDataToBytes(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(got, []byte("null")) {
		t.Fatalf("want %q, got %q", "null", string(got))
	}
}

func TestConvertAnyDataToBytes_UnsupportedType_Error(t *testing.T) {
	in := make(chan int) // encoding/json cannot marshal channels
	_, err := ConvertAnyDataToBytes(in)
	if err == nil {
		t.Fatalf("expected error for unsupported type, got nil")
	}
}
