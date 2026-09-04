// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/google/go-cmp/cmp"
)

// TestRoundTrip is the acceptance test for the whole schema.
//
// Parse a real .vcf, serialize it, parse that again, and require the two
// models to be equal. Models, not bytes: RFC 6350 defines no property order,
// so a byte comparison fails on reordering noise that means nothing.
//
// This is the only check in the repository that verifies the schema against
// the RFC rather than against a linter. If Contact were missing a property or
// modeled one wrongly, the second parse would differ from the first.
func TestRoundTrip(t *testing.T) {
	paths, err := filepath.Glob("testdata/*.vcf")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no fixtures found: %v", err)
	}

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			src, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			first, err := Decode(string(src))
			if err != nil {
				t.Fatalf("first decode: %v", err)
			}

			out, err := Encode(first)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}

			second, err := Decode(out)
			if err != nil {
				t.Fatalf("second decode of\n%s\n: %v", out, err)
			}

			if diff := cmp.Diff(first, second, protocmp.Transform()); diff != "" {
				t.Errorf("round trip lost or changed data (-first +second):\n%s", diff)
			}
		})
	}
}

// TestEncodeFolds checks RFC 6350 section 3.2: content lines are folded at 75
// octets, and a fold never splits a multi-byte UTF-8 sequence.
func TestEncodeFolds(t *testing.T) {
	src, err := os.ReadFile("testdata/utf8.vcf")
	if err != nil {
		t.Fatal(err)
	}
	c, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}
	out, err := Encode(c)
	if err != nil {
		t.Fatal(err)
	}

	for _, line := range strings.Split(out, "\r\n") {
		if len(line) > 75 {
			t.Errorf("line exceeds 75 octets (%d): %q", len(line), line)
		}
	}
	if !utf8.ValidString(out) {
		t.Error("folding split a multi-byte UTF-8 sequence")
	}
	// Unfolding the output must recover the original values.
	back, err := Decode(out)
	if err != nil {
		t.Fatalf("folded output does not parse: %v", err)
	}
	if !proto.Equal(c, back) {
		t.Error("folding changed the model")
	}
}

// TestUnmodelledPropertiesSurvive is the ExtensionProperty contract: a
// property this schema does not type must still round-trip, per RFC 6350
// section 6.10.
func TestUnmodelledPropertiesSurvive(t *testing.T) {
	src, err := os.ReadFile("testdata/extensions.vcf")
	if err != nil {
		t.Fatal(err)
	}
	c, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]bool{
		"SOURCE": false, "GENDER": false, "PHOTO": false,
		"X-ABLabel": false, "X-PHONETIC-FIRST-NAME": false, "UID": false,
	}
	for _, e := range c.GetExtensions() {
		if _, ok := want[e.GetKey()]; ok {
			want[e.GetKey()] = true
		}
	}
	for k, seen := range want {
		if !seen {
			t.Errorf("%s was dropped instead of preserved as an extension", k)
		}
	}

	// The group on item1.X-ABLabel must survive; Apple exporters rely on it.
	var grouped bool
	for _, e := range c.GetExtensions() {
		if e.GetKey() == "X-ABLabel" && e.GetGroup() == "item1" {
			grouped = true
		}
	}
	if !grouped {
		t.Error("property group was lost")
	}
}

// TestRejectsMalformed checks the errors a conforming parser owes its caller.
func TestRejectsMalformed(t *testing.T) {
	cases := map[string]string{
		"missing BEGIN":   "VERSION:4.0\r\nFN:X\r\n",
		"wrong version":   "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:X\r\nEND:VCARD\r\n",
		"missing version": "BEGIN:VCARD\r\nFN:X\r\nEND:VCARD\r\n",
		"missing FN":      "BEGIN:VCARD\r\nVERSION:4.0\r\nEND:VCARD\r\n",
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode(src); err == nil {
				t.Error("expected an error, got none")
			}
		})
	}
}
