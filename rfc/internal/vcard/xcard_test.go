// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
)

// TestXCardCrossFormat: text/vcard and xCard must produce the same Contact.
//
// This is the third encoding of one data model, and the one that reshapes it
// most -- structured values become *named* elements rather than positions.
// If Contact models RFC 6350 correctly, all three paths agree.
func TestXCardCrossFormat(t *testing.T) {
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
			x, err := EncodeXCard(fromText)
			if err != nil {
				t.Fatalf("encode xcard: %v", err)
			}
			fromXML, err := DecodeXCard(x)
			if err != nil {
				t.Fatalf("decode xcard:\n%s\n%v", x, err)
			}
			// Two legitimate differences, both properties of the format
			// rather than bugs.
			//
			// One: xCard lowercases property names, section 3.2, so an
			// extension key's original case cannot survive -- the same
			// constraint jCard has.
			//
			// Two: RFC 9554's added N and ADR components have no xCard
			// representation. RFC 6351 maps compound values to *named* child
			// elements, <surname>, <street> and so on, and RFC 9554 defines
			// its components only for the text syntax -- it registers no XML
			// element names for them. Inventing names here would emit xCard
			// no other implementation could read, so the components are
			// dropped on the way into XML and this test says so out loud.
			// jCard has no such problem: RFC 7095 section 3.3.1.3 writes a
			// compound value as a positional array, which simply gets longer.
			opts := cmp.Options{
				protocmp.Transform(),
				protocmp.FilterField(&vcardv1.ExtensionProperty{}, "key",
					cmp.Comparer(strings.EqualFold)),
				protocmp.IgnoreFields(&vcardv1.Address{},
					"rooms", "apartments", "floors", "street_numbers", "streets",
					"buildings", "blocks", "subdistricts", "districts",
					"landmarks", "directions"),
				protocmp.IgnoreFields(&vcardv1.Name{}, "secondary_surnames", "generations"),
			}
			if diff := cmp.Diff(fromText, fromXML, opts); diff != "" {
				t.Errorf("text and xCard disagree (-text +xml):\n%s\nxcard was:\n%s", diff, x)
			}
		})
	}
}

// TestXCardShape checks the document against RFC 6351.
func TestXCardShape(t *testing.T) {
	src, err := os.ReadFile("testdata/rfc6350_example.vcf")
	if err != nil {
		t.Fatal(err)
	}
	c, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeXCard(c)
	if err != nil {
		t.Fatal(err)
	}

	var doc node
	if err := xml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("not XML: %v", err)
	}
	if doc.XMLName.Local != "vcards" {
		t.Errorf("root = <%s>, want <vcards>", doc.XMLName.Local)
	}
	// Section 4: the namespace carries the version, so there is no VERSION
	// property and its absence is required rather than incidental.
	if doc.XMLName.Space != Namespace {
		t.Errorf("namespace = %q, want %q", doc.XMLName.Space, Namespace)
	}
	if strings.Contains(string(raw), "<version>") {
		t.Error("xCard must not carry a VERSION property; the namespace replaces it")
	}

	card, ok := doc.child("vcard")
	if !ok {
		t.Fatal("no <vcard>")
	}

	// Section 3.3: N uses named child elements, in the RFC's order.
	n, ok := card.child("n")
	if !ok {
		t.Fatal("no <n>")
	}
	got := make([]string, 0, len(n.Children))
	for _, c := range n.Children {
		got = append(got, c.XMLName.Local)
	}
	want := []string{"surname", "given", "additional", "prefix", "suffix", "suffix"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("<n> children = %v, want %v", got, want)
	}
	if s, _ := n.child("surname"); s.Chardata != "Perreault" {
		t.Errorf("<surname> = %q", s.Chardata)
	}

	// A tel: URI is wrapped in <uri>, not <text>.
	tel, ok := card.child("tel")
	if !ok {
		t.Fatal("no <tel>")
	}
	if _, hasURI := tel.child("uri"); !hasURI {
		t.Error("tel: value should be wrapped in <uri>")
	}
	// Parameters are nested and each value is type-wrapped.
	params, ok := tel.child("parameters")
	if !ok {
		t.Fatal("<tel> has no <parameters>")
	}
	typ, ok := params.child("type")
	if !ok {
		t.Fatal("<parameters> has no <type>")
	}
	if len(typ.Children) == 0 || typ.Children[0].XMLName.Local != "text" {
		t.Errorf("<type> value is not wrapped in <text>: %v", typ.Children)
	}
}

// TestXCardNamedComponentsAreOrderIndependent: XML may present named children
// in any order, so the decoder must read them by name and not by position.
func TestXCardNamedComponentsAreOrderIndependent(t *testing.T) {
	shuffled := []byte(xml.Header + `<vcards xmlns="` + Namespace + `">
  <vcard>
    <fn><text>Order Test</text></fn>
    <n>
      <suffix>PhD</suffix>
      <given>Ada</given>
      <surname>Lovelace</surname>
    </n>
  </vcard>
</vcards>`)
	c, err := DecodeXCard(shuffled)
	if err != nil {
		t.Fatal(err)
	}
	n := c.GetNameComponents()
	if got := n.GetFamilyName(); len(got) != 1 || got[0] != "Lovelace" {
		t.Errorf("family = %q, want Lovelace", got)
	}
	if got := n.GetGivenName(); len(got) != 1 || got[0] != "Ada" {
		t.Errorf("given = %q, want Ada", got)
	}
	if got := n.GetHonorificSuffixes(); len(got) != 1 || got[0] != "PhD" {
		t.Errorf("suffix = %q, want PhD", got)
	}
}

// TestXCardGroups: a property group becomes a <group name="..."> wrapper.
func TestXCardGroups(t *testing.T) {
	src, err := os.ReadFile("testdata/extensions.vcf")
	if err != nil {
		t.Fatal(err)
	}
	c, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeXCard(c)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `<group name="item1">`) {
		t.Errorf("property group not written as a <group> element:\n%s", raw)
	}
	back, err := DecodeXCard(raw)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, e := range back.GetExtensions() {
		if strings.EqualFold(e.GetKey(), "X-ABLabel") && e.GetGroup() == "item1" {
			found = true
		}
	}
	if !found {
		t.Error("group did not survive the round trip")
	}
}
