// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
)

// TestJCardCrossFormat is the point of having two codecs.
//
// Take every text/vcard fixture, decode it, re-encode it as jCard, decode
// that, and require the two models to be equal. RFC 7095 defines jCard as an
// alternate encoding of RFC 6350's data model, so if Contact models that
// model correctly the two paths cannot disagree. A difference here means one
// of the codecs is wrong about the shared model, which is exactly the class
// of bug a single-format round trip cannot see.
func TestJCardCrossFormat(t *testing.T) {
	paths, _ := filepath.Glob("testdata/*.vcf")
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			src, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			fromText, err := Decode(string(src))
			if err != nil {
				t.Fatal(err)
			}

			j, err := EncodeJCard(fromText)
			if err != nil {
				t.Fatalf("encode jcard: %v", err)
			}
			fromJSON, err := DecodeJCard(j)
			if err != nil {
				t.Fatalf("decode jcard %s: %v", j, err)
			}

			// jCard mandates lowercase property names (section 3.3), so an
			// extension key's original case cannot survive the crossing.
			// That is a property of the format, not a defect here, and it is
			// the only difference the two encodings may legitimately have.
			opts := cmp.Options{
				protocmp.Transform(),
				protocmp.FilterField(&vcardv1.ExtensionProperty{}, "key",
					cmp.Comparer(strings.EqualFold)),
			}
			if diff := cmp.Diff(fromText, fromJSON, opts); diff != "" {
				t.Errorf("text and jCard disagree (-text +json):\n%s", diff)
			}
		})
	}
}

// TestJCardShape checks the wire format itself against RFC 7095 section 3.
func TestJCardShape(t *testing.T) {
	src, err := os.ReadFile("testdata/rfc6350_example.vcf")
	if err != nil {
		t.Fatal(err)
	}
	c, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeJCard(c)
	if err != nil {
		t.Fatal(err)
	}

	var doc []any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("output is not JSON: %v", err)
	}
	if len(doc) != 2 || doc[0] != "vcard" {
		t.Fatalf(`want ["vcard", [...]], got %v`, doc[0])
	}
	props, ok := doc[1].([]any)
	if !ok || len(props) == 0 {
		t.Fatal("second element is not a non-empty array")
	}

	// Section 3.3: version MUST be the first property.
	first, _ := props[0].([]any)
	if len(first) < 4 || first[0] != "version" || first[3] != "4.0" {
		t.Errorf(`first property = %v, want ["version",{},"text","4.0"]`, first)
	}

	for _, p := range props {
		arr, ok := p.([]any)
		if !ok || len(arr) < 4 {
			t.Errorf("property is not a 4+ element array: %v", p)
			continue
		}
		name, _ := arr[0].(string)
		if name != strings.ToLower(name) {
			t.Errorf("property name %q is not lowercase", name)
		}
		if _, ok := arr[1].(map[string]any); !ok {
			t.Errorf("%s: parameters is not an object: %v", name, arr[1])
		}
		if _, ok := arr[2].(string); !ok {
			t.Errorf("%s: type identifier is not a string: %v", name, arr[2])
		}
		// N and ADR are structured: their value is a nested array.
		if name == "n" || name == "adr" {
			if _, ok := arr[3].([]any); !ok {
				t.Errorf("%s: structured value is not an array: %v", name, arr[3])
			}
		}
		// BEGIN and END have no jCard representation.
		if name == "begin" || name == "end" {
			t.Errorf("%s should not appear as a jCard property", name)
		}
	}
}
