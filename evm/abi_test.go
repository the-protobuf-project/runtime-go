package evm

import (
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"

	"github.com/the-protobuf-project/runtime-go/store"
)

// bookABI is a complete-enough ABI: create (so inputTuple resolves) and get,
// both over the same record tuple covering string, int32, enum(uint8), and
// uint256-timestamp components.
const bookABI = `[
  {"type":"function","name":"create","stateMutability":"nonpayable","outputs":[],"inputs":[
    {"name":"record","type":"tuple","components":[
      {"name":"id","type":"string"},
      {"name":"name","type":"string"},
      {"name":"title","type":"string"},
      {"name":"authorID","type":"string"},
      {"name":"publishedYear","type":"int32"},
      {"name":"genre","type":"uint8"},
      {"name":"createTime","type":"uint256"}
    ]}]},
  {"type":"function","name":"get","stateMutability":"view","inputs":[{"name":"key","type":"string"}],"outputs":[
    {"name":"","type":"tuple","components":[
      {"name":"id","type":"string"},
      {"name":"name","type":"string"},
      {"name":"title","type":"string"},
      {"name":"authorID","type":"string"},
      {"name":"publishedYear","type":"int32"},
      {"name":"genre","type":"uint8"},
      {"name":"createTime","type":"uint256"}
    ]}]}
]`

func bookResource() *store.Resource {
	return &store.Resource{
		Name:     "Book",
		PKColumn: "id",
		Columns: []store.Column{
			{Name: "id", Kind: store.KindString, PrimaryKey: true, NotNull: true},
			{Name: "name", Kind: store.KindString, NotNull: true},
			{Name: "title", Kind: store.KindString, NotNull: true},
			{Name: "author_id", Kind: store.KindString, NotNull: true},
			{Name: "published_year", Kind: store.KindInt},
			{Name: "genre", Kind: store.KindEnum, NotNull: true},
			{Name: "create_time", Kind: store.KindTimestamp, NotNull: true},
		},
	}
}

func TestABIRecordRoundTrip(t *testing.T) {
	parsed, err := parseABI(bookABI)
	if err != nil {
		t.Fatalf("parse ABI: %v", err)
	}
	tt, err := inputTuple(parsed, "create")
	if err != nil {
		t.Fatalf("input tuple: %v", err)
	}
	res := bookResource()
	cols := map[string]any{
		"id":             "books/dune",
		"name":           "books/dune",
		"title":          "Dune",
		"author_id":      "authors/herbert",
		"published_year": int64(1965),
		"genre":          int32(2),
		"create_time":    time.Unix(1700000000, 0).UTC(),
	}

	tuple, err := encodeRecord(tt, res, cols)
	if err != nil {
		t.Fatalf("encodeRecord: %v", err)
	}

	// Pack then unpack through the real ABI codec to prove the encoding is valid.
	args := abi.Arguments{{Type: tt}}
	packed, err := args.Pack(tuple)
	if err != nil {
		t.Fatalf("ABI pack: %v", err)
	}
	unpacked, err := args.Unpack(packed)
	if err != nil {
		t.Fatalf("ABI unpack: %v", err)
	}

	got, err := decodeRecord(res, unpacked[0])
	if err != nil {
		t.Fatalf("decodeRecord: %v", err)
	}

	for k, want := range cols {
		if g := got[k]; g != want {
			t.Errorf("column %q round-trip = %#v, want %#v", k, g, want)
		}
	}
}

func TestToBigTimestamp(t *testing.T) {
	col := store.Column{Kind: store.KindTimestamp}
	n, err := toBig(col, time.Unix(42, 0))
	if err != nil || n.Cmp(big.NewInt(42)) != 0 {
		t.Fatalf("toBig timestamp = %v, %v; want 42", n, err)
	}
}

func TestSchemaMatches(t *testing.T) {
	var v [32]byte
	for i := range v {
		v[i] = byte(i)
	}
	// 0x000102...1f
	hexStr := "0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	if !schemaMatches(v, hexStr) {
		t.Fatal("schemaMatches: identical fingerprints should match")
	}
	if schemaMatches(v, "0xdead") {
		t.Fatal("schemaMatches: malformed want should not match")
	}
	var other [32]byte
	other[0] = 0xff
	if schemaMatches(other, hexStr) {
		t.Fatal("schemaMatches: differing fingerprints should not match")
	}
}
