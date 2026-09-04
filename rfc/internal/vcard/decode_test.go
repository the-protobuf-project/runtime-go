// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"os"
	"strings"
	"testing"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
)

// TestDecodeValues asserts what specific properties decode to.
//
// This exists because TestRoundTrip cannot catch symmetric loss. When the
// parser dropped TYPE="work,voice" it also failed to emit it, so decode ->
// encode -> decode stayed equal while the data was gone. Round-trip equality
// proves stability, not correctness; only asserting values proves correctness.
func TestDecodeValues(t *testing.T) {
	src, err := os.ReadFile("testdata/rfc6350_example.vcf")
	if err != nil {
		t.Fatal(err)
	}
	c, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}

	if got := c.GetDisplayNames(); len(got) != 1 || got[0] != "Simon Perreault" {
		t.Errorf("FN = %q", got)
	}

	// N is compound, and its fifth component is comma-separated.
	n := c.GetNameComponents()
	if got := n.GetFamilyName(); len(got) != 1 || got[0] != "Perreault" {
		t.Errorf("N family = %q", got)
	}
	if got := n.GetHonorificSuffixes(); len(got) != 2 ||
		got[0] != "ing. jr" || got[1] != "M.Sc." {
		t.Errorf("N suffixes = %q, want two values", got)
	}

	// TYPE="work,voice" is one quoted parameter holding two values, per the
	// example in section 6.4.1.
	if len(c.GetTelephones()) != 1 {
		t.Fatalf("want 1 TEL, got %d", len(c.GetTelephones()))
	}
	tel := c.GetTelephones()[0]
	if got := tel.GetTypes(); len(got) != 1 || got[0] != vcardv1.Type_TYPE_WORK {
		t.Errorf("TEL types = %v, want [WORK]", got)
	}
	if got := tel.GetFeatures(); len(got) != 1 || got[0] != vcardv1.Feature_FEATURE_VOICE {
		t.Errorf("TEL features = %v, want [VOICE]", got)
	}
	if tel.GetPref() != 1 {
		t.Errorf("TEL PREF = %d, want 1", tel.GetPref())
	}
	// A tel: URI takes the uri branch, not text.
	if tel.GetUri() == "" {
		t.Error("tel: URI decoded as text instead of uri")
	}

	// ADR is compound with seven components; the first is empty here.
	if len(c.GetAddresses()) != 1 {
		t.Fatalf("want 1 ADR, got %d", len(c.GetAddresses()))
	}
	adr := c.GetAddresses()[0]
	if got := adr.GetStreetAddresses(); len(got) != 1 || got[0] != "2875 Laurier" {
		t.Errorf("ADR street = %q", got)
	}
	if got := adr.GetCountries(); len(got) != 1 || got[0] != "Canada" {
		t.Errorf("ADR country = %q", got)
	}

	// ORG is compound: first component is the name, the rest are units.
	if len(c.GetOrganizations()) != 1 {
		t.Fatalf("want 1 ORG, got %d", len(c.GetOrganizations()))
	}
	org := c.GetOrganizations()[0]
	if org.GetValue() != "Viagenie" {
		t.Errorf("ORG value = %q", org.GetValue())
	}
	if got := org.GetUnits(); len(got) != 1 || got[0] != "Research" {
		t.Errorf("ORG units = %q", got)
	}
}

// TestDecodeNewProperties asserts values for the properties added after the
// initial nine: BDAY/ANNIVERSARY, NICKNAME, URL, CATEGORIES, ROLE, IMPP,
// LANG, GEO, TZ and RELATED. Same reasoning as TestDecodeValues: round-trip
// equality would stay green even if a value were dropped and re-dropped
// identically, so specific values are asserted instead.
func TestDecodeNewProperties(t *testing.T) {
	src, err := os.ReadFile("testdata/new_properties.vcf")
	if err != nil {
		t.Fatal(err)
	}
	c, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}

	// BDAY:--0415 is section 4.3.1's no-year form: a birthday, month and day
	// only.
	bday := c.GetBirthday().GetDate()
	if bday.GetYear() != 0 || bday.GetMonth() != 4 || bday.GetDay() != 15 {
		t.Errorf("BDAY = %+v, want year=0 month=4 day=15", bday)
	}
	anniv := c.GetAnniversary().GetDate()
	if anniv.GetYear() != 2009 || anniv.GetMonth() != 8 || anniv.GetDay() != 12 {
		t.Errorf("ANNIVERSARY = %+v, want 2009-08-12", anniv)
	}

	if len(c.GetNicknames()) != 1 {
		t.Fatalf("want 1 NICKNAME, got %d", len(c.GetNicknames()))
	}
	nick := c.GetNicknames()[0]
	if got := nick.GetValues(); len(got) != 2 || got[0] != "Boss" || got[1] != "Chief" {
		t.Errorf("NICKNAME values = %q, want [Boss Chief]", got)
	}
	if nick.GetPref() != 1 {
		t.Errorf("NICKNAME PREF = %d, want 1", nick.GetPref())
	}

	if len(c.GetUrls()) != 1 || c.GetUrls()[0].GetValue() != "https://example.com/~jdoe" {
		t.Errorf("URL = %+v", c.GetUrls())
	}

	if got := c.GetCategories(); len(got) != 3 || got[2] != "INDUSTRY" {
		t.Errorf("CATEGORIES = %q, want 3 values ending INDUSTRY", got)
	}

	if len(c.GetRoles()) != 1 || c.GetRoles()[0].GetValue() != "Executive" {
		t.Errorf("ROLE = %+v", c.GetRoles())
	}

	if len(c.GetInstantMessages()) != 1 ||
		c.GetInstantMessages()[0].GetValue() != "xmpp:alice@example.com" {
		t.Errorf("IMPP = %+v", c.GetInstantMessages())
	}

	if len(c.GetLanguages()) != 1 || c.GetLanguages()[0].GetValue() != "en-US" {
		t.Errorf("LANG = %+v", c.GetLanguages())
	}

	// The coordinate comma in a geo: URI must survive unescaped: it is a URI
	// value, not a TEXT value, so section 3.4's escaping does not apply.
	if got := c.GetLocations(); len(got) != 1 || got[0].GetValue() != "geo:37.386013,-122.082932" {
		t.Errorf("GEO = %q, want unescaped coordinate comma", got)
	}

	// Three TZ lines, one per section 6.5.1 form.
	if len(c.GetTimezones()) != 3 {
		t.Fatalf("want 3 TZ, got %d", len(c.GetTimezones()))
	}
	if got := c.GetTimezones()[0].GetText(); got != "America/New_York" {
		t.Errorf("TZ[0] text = %q", got)
	}
	if got := c.GetTimezones()[1].GetUri(); got != "https://example.com/tz/edt" {
		t.Errorf("TZ[1] uri = %q", got)
	}
	if got := c.GetTimezones()[2].GetUtcOffset(); got != "-0500" {
		t.Errorf("TZ[2] utc_offset = %q", got)
	}

	// Two RELATED lines: the RFC's own uri and VALUE=text examples.
	if len(c.GetRelations()) != 2 {
		t.Fatalf("want 2 RELATED, got %d", len(c.GetRelations()))
	}
	rel0 := c.GetRelations()[0]
	if got := rel0.GetUri(); got != "urn:uuid:f81d4fae-7dec-11d0-a765-00a0c91e6bf6" {
		t.Errorf("RELATED[0] uri = %q", got)
	}
	if got := rel0.GetRelationTypes(); len(got) != 1 || got[0] != vcardv1.RelationType_RELATION_TYPE_FRIEND {
		t.Errorf("RELATED[0] types = %v, want [FRIEND]", got)
	}
	rel1 := c.GetRelations()[1]
	want := "Please contact my assistant Jane Doe for any inquiries."
	if got := rel1.GetText(); got != want {
		t.Errorf("RELATED[1] text = %q, want %q", got, want)
	}
	if got := rel1.GetRelationTypes(); len(got) != 1 || got[0] != vcardv1.RelationType_RELATION_TYPE_CO_WORKER {
		t.Errorf("RELATED[1] types = %v, want [CO_WORKER]", got)
	}
}

// TestDecodeEscapes covers RFC 6350 section 3.4.
func TestDecodeEscapes(t *testing.T) {
	src, err := os.ReadFile("testdata/escapes.vcf")
	if err != nil {
		t.Fatal(err)
	}
	c, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}

	if got := c.GetDisplayNames()[0]; got != "Escapes, Tricky" {
		t.Errorf("escaped comma in FN = %q", got)
	}
	if got := c.GetNameComponents().GetFamilyName(); len(got) != 1 || got[0] != "Semi;colon" {
		t.Errorf("escaped semicolon in N = %q", got)
	}
	want := "Line one\nLine two; and a semicolon, and a comma\\ and a backslash"
	if got := c.GetNotes()[0]; got != want {
		t.Errorf("NOTE escapes:\n got %q\nwant %q", got, want)
	}
}

// TestRfc9554Components asserts the components RFC 9554 added to N and ADR.
//
// Before RFC 9554 support, decodeAddress read seven components and stopped:
// the parser produced all eighteen and eleven were thrown away, so an address
// carrying a room, floor or landmark lost them without error. This asserts
// values rather than round-trip stability, because the encoder was equally
// blind and the two agreed with each other while the data was gone.
func TestRfc9554Components(t *testing.T) {
	src, err := os.ReadFile("testdata/rfc9554_extended.vcf")
	if err != nil {
		t.Fatal(err)
	}
	c, err := Decode(string(src))
	if err != nil {
		t.Fatal(err)
	}

	// N components 5 and 6, RFC 9554 section 2.2.
	n := c.GetNameComponents()
	if got := n.GetSecondarySurnames(); len(got) != 1 || got[0] != "García" {
		t.Errorf("N secondary surname = %q, want [García]", got)
	}
	if got := n.GetGenerations(); len(got) != 1 || got[0] != "Jr" {
		t.Errorf("N generation = %q, want [Jr]", got)
	}
	// The RFC 6350 components must be untouched by the extension.
	if got := n.GetHonorificSuffixes(); len(got) != 1 || got[0] != "PhD" {
		t.Errorf("N suffixes = %q, want [PhD]", got)
	}

	if len(c.GetAddresses()) != 2 {
		t.Fatalf("got %d addresses, want 2", len(c.GetAddresses()))
	}
	a := c.GetAddresses()[0]

	// ADR components 7 to 17, RFC 9554 section 2.1.
	for _, tc := range []struct {
		name string
		got  []string
		want string
	}{
		{"room", a.GetRooms(), "Room 4"},
		{"apartment", a.GetApartments(), "Apt 2B"},
		{"floor", a.GetFloors(), "3"},
		{"street number", a.GetStreetNumbers(), "1"},
		{"street", a.GetStreets(), "Example Way"},
		{"building", a.GetBuildings(), "Tower A"},
		{"block", a.GetBlocks(), "Block C"},
		{"subdistrict", a.GetSubdistricts(), "Northside"},
		{"district", a.GetDistricts(), "Sangamon"},
		{"landmark", a.GetLandmarks(), "By the fountain"},
		{"direction", a.GetDirections(), "NW"},
	} {
		if len(tc.got) != 1 || tc.got[0] != tc.want {
			t.Errorf("ADR %s = %q, want [%s]", tc.name, tc.got, tc.want)
		}
	}

	// TYPE=billing, RFC 9554 section 5.
	if got := a.GetTypes(); len(got) != 1 || got[0] != vcardv1.Type_TYPE_BILLING {
		t.Errorf("ADR types = %v, want [TYPE_BILLING]", got)
	}

	// A seven-component ADR must still decode, with the new components empty.
	plain := c.GetAddresses()[1]
	if got := plain.GetStreetAddresses(); len(got) != 1 || got[0] != "9 Plain Street" {
		t.Errorf("plain ADR street = %q", got)
	}
	if len(plain.GetRooms()) != 0 || len(plain.GetDirections()) != 0 {
		t.Error("plain ADR gained extended components it does not have")
	}
}

// TestRfc9554EncodeIsConditional asserts that a contact with no RFC 9554
// components encodes to the same seven-component ADR and five-component N it
// always did. Appending empty components unconditionally would change the
// bytes of every vCard in existence for no gain.
func TestRfc9554EncodeIsConditional(t *testing.T) {
	src, err := os.ReadFile("testdata/rfc6350_example.vcf")
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
		name, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		// Count separators in the value only: the parameter list carries its
		// own semicolons, e.g. ADR;TYPE=work:...
		switch {
		case name == "N":
			if n := strings.Count(value, ";"); n != 4 {
				t.Errorf("N has %d component separators, want 4: %q", n, line)
			}
		case name == "ADR" || strings.HasPrefix(name, "ADR;"):
			if n := strings.Count(value, ";"); n != 6 {
				t.Errorf("ADR has %d component separators, want 6: %q", n, line)
			}
		}
	}
}

// TestQuotedParamIsOneValue pins the two halves of the quoted-parameter rule
// against each other, because they pull in opposite directions.
//
// RFC 5545 section 3.1 and RFC 6350 section 3.3 both make a quoted-string one
// value, so a comma inside it is content. But RFC 6350 section 6.4.1's own
// example is TEL;TYPE="work,voice" meaning two types, which its own grammar
// does not permit -- the example and the ABNF disagree, and real vCards
// follow the example. So TYPE is split inside quotes and free-text parameters
// are not. Getting either half wrong is silent: one drops a type, the other
// truncates a name.
func TestQuotedParamIsOneValue(t *testing.T) {
	src := "BEGIN:VCARD\r\n" +
		"VERSION:4.0\r\n" +
		"FN:John Doe\r\n" +
		"ADR;TYPE=\"work,home\";LABEL=\"1 Main St, Springfield\":;;1 Main St;Springfield;IL;62704;USA\r\n" +
		"END:VCARD\r\n"
	c, err := Decode(src)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.GetAddresses()) != 1 {
		t.Fatalf("got %d addresses, want 1", len(c.GetAddresses()))
	}
	a := c.GetAddresses()[0]

	// TYPE is a list parameter: the quoted comma separates two values.
	if got := a.GetTypes(); len(got) != 2 {
		t.Errorf("TYPE = %v, want two values", got)
	}

	// LABEL is free text: its comma is content, not a separator.
	if got := a.GetLabel(); got != "1 Main St, Springfield" {
		t.Errorf("LABEL = %q, want the comma intact", got)
	}

	// And it must survive re-encoding, which means being re-quoted.
	out, err := Encode(c)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `LABEL="1 Main St, Springfield"`) {
		t.Errorf("LABEL lost its quoting on encode:\n%s", out)
	}
	again, err := Decode(out)
	if err != nil {
		t.Fatal(err)
	}
	if got := again.GetAddresses()[0].GetLabel(); got != "1 Main St, Springfield" {
		t.Errorf("LABEL after round trip = %q", got)
	}
}
