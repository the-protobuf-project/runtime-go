// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package jscontact

import (
	"testing"

	"google.golang.org/genproto/googleapis/type/date"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc9553/card/v1"
)

// TestNameMergeRule pins RFC 9555 section 2.5.5's Table 1, which is the
// subtlest rule in the conversion.
//
// Two components are *merged* on the way to vCard -- surname2 into Family
// name, generation into Honorific suffix -- so a reader that predates RFC
// 9554 still sees the whole name. Coming back, a value present in both the
// merged slot and its dedicated component must be taken once.
//
// Without that dedupe the surname doubles on every round trip, and it doubles
// silently: the vCard is still valid and the Card still parses.
func TestNameMergeRule(t *testing.T) {
	card := &cardv1.Card{
		NameComponents: &cardv1.Name{
			DisplayName: "Jane Rodriguez Garcia Jr",
			Components: []*cardv1.NameComponent{
				{Kind: cardv1.NameComponentKind_NAME_COMPONENT_KIND_GIVEN, Value: "Jane"},
				{Kind: cardv1.NameComponentKind_NAME_COMPONENT_KIND_SURNAME, Value: "Rodriguez"},
				{Kind: cardv1.NameComponentKind_NAME_COMPONENT_KIND_SURNAME2, Value: "Garcia"},
				{Kind: cardv1.NameComponentKind_NAME_COMPONENT_KIND_GENERATION, Value: "Jr"},
				{Kind: cardv1.NameComponentKind_NAME_COMPONENT_KIND_CREDENTIAL, Value: "PhD"},
			},
		},
	}

	c, err := ToVcard(card)
	if err != nil {
		t.Fatal(err)
	}
	n := c.GetNameComponents()

	// The merge: Family name carries both surnames, Honorific suffix both
	// the credential and the generation.
	if got := n.GetFamilyName(); len(got) != 2 || got[0] != "Rodriguez" || got[1] != "Garcia" {
		t.Errorf("family name = %q, want [Rodriguez Garcia] after the merge", got)
	}
	if got := n.GetHonorificSuffixes(); len(got) != 2 || got[0] != "PhD" || got[1] != "Jr" {
		t.Errorf("honorific suffixes = %q, want [PhD Jr] after the merge", got)
	}
	// And the dedicated RFC 9554 components are populated too.
	if got := n.GetSecondarySurnames(); len(got) != 1 || got[0] != "Garcia" {
		t.Errorf("secondary surname = %q", got)
	}

	// Back again: the merged copies must not produce duplicates.
	back, err := FromVcard(c)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[cardv1.NameComponentKind][]string{}
	for _, comp := range back.GetNameComponents().GetComponents() {
		counts[comp.GetKind()] = append(counts[comp.GetKind()], comp.GetValue())
	}
	if got := counts[cardv1.NameComponentKind_NAME_COMPONENT_KIND_SURNAME]; len(got) != 1 || got[0] != "Rodriguez" {
		t.Errorf("surname after round trip = %q, want just [Rodriguez]", got)
	}
	if got := counts[cardv1.NameComponentKind_NAME_COMPONENT_KIND_SURNAME2]; len(got) != 1 || got[0] != "Garcia" {
		t.Errorf("surname2 after round trip = %q", got)
	}
	if got := counts[cardv1.NameComponentKind_NAME_COMPONENT_KIND_CREDENTIAL]; len(got) != 1 || got[0] != "PhD" {
		t.Errorf("credential after round trip = %q, want just [PhD]", got)
	}
	if got := counts[cardv1.NameComponentKind_NAME_COMPONENT_KIND_GENERATION]; len(got) != 1 || got[0] != "Jr" {
		t.Errorf("generation after round trip = %q", got)
	}
}

// TestAddressFormatSwitch pins RFC 9555 section 2.6.1's conditional: the
// legacy `street address` and `extended address` slots convert only when the
// ADR is *not* in RFC 9554's extended form.
//
// Reading both would duplicate every street, since ToVcard deliberately fills
// the legacy slots so an RFC 6350 reader still sees an address.
func TestAddressFormatSwitch(t *testing.T) {
	t.Run("extended form ignores the legacy slots", func(t *testing.T) {
		c := &vcardv1.Contact{
			DisplayNames: []string{"Test"},
			Addresses: []*vcardv1.Address{{
				Streets:         []string{"Example Way"},
				StreetNumbers:   []string{"1"},
				Localities:      []string{"Springfield"},
				StreetAddresses: []string{"1 Example Way"}, // the legacy duplicate
			}},
		}
		card, err := FromVcard(c)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, comp := range card.GetAddresses()["adr1"].GetComponents() {
			if comp.GetKind() == cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_NAME {
				names = append(names, comp.GetValue())
			}
		}
		if len(names) != 1 || names[0] != "Example Way" {
			t.Errorf("street components = %q, want just [Example Way]; the legacy slot leaked", names)
		}
	})

	t.Run("legacy form uses the legacy slots", func(t *testing.T) {
		c := &vcardv1.Contact{
			DisplayNames: []string{"Test"},
			Addresses: []*vcardv1.Address{{
				StreetAddresses:   []string{"1 Example Way"},
				ExtendedAddresses: []string{"Flat 2"},
				Localities:        []string{"Springfield"},
			}},
		}
		card, err := FromVcard(c)
		if err != nil {
			t.Fatal(err)
		}
		found := map[cardv1.AddressComponentKind]string{}
		for _, comp := range card.GetAddresses()["adr1"].GetComponents() {
			found[comp.GetKind()] = comp.GetValue()
		}
		if got := found[cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_NAME]; got != "1 Example Way" {
			t.Errorf("street address = %q, want the legacy slot to convert", got)
		}
		if got := found[cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_APARTMENT]; got != "Flat 2" {
			t.Errorf("extended address = %q, want it mapped to apartment", got)
		}
	})

	t.Run("round trip does not duplicate the street", func(t *testing.T) {
		card := &cardv1.Card{
			NameComponents: &cardv1.Name{DisplayName: "Test"},
			Addresses: map[string]*cardv1.Address{"adr1": {
				Components: []*cardv1.AddressComponent{
					{Kind: cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_NUMBER, Value: "1"},
					{Kind: cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_NAME, Value: "Example Way"},
				},
			}},
		}
		c, err := ToVcard(card)
		if err != nil {
			t.Fatal(err)
		}
		// The legacy slot is filled for old readers...
		if got := c.GetAddresses()[0].GetStreetAddresses(); len(got) != 1 {
			t.Errorf("legacy street slot = %q, want it populated for RFC 6350 readers", got)
		}
		// ...but must not come back as a second component.
		back, err := FromVcard(c)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, comp := range back.GetAddresses()["adr1"].GetComponents() {
			if comp.GetKind() == cardv1.AddressComponentKind_ADDRESS_COMPONENT_KIND_NAME {
				n++
			}
		}
		if n != 1 {
			t.Errorf("got %d street-name components after a round trip, want 1", n)
		}
	})
}

// TestUidIsNotGenerated pins the one place this codec deliberately departs
// from RFC 9555.
//
// Section 2.1.1 says an implementation MUST generate a uid when the vCard has
// none. RFC 9982 then made uid optional precisely to stop that: a generated
// value differs every run, so re-importing the same vCard creates a duplicate
// instead of matching the existing record. The later RFC wins.
func TestUidIsNotGenerated(t *testing.T) {
	c := &vcardv1.Contact{DisplayNames: []string{"No Uid"}}
	card, err := FromVcard(c)
	if err != nil {
		t.Fatal(err)
	}
	if got := card.GetJscontactUid(); got != "" {
		t.Errorf("jscontact_uid = %q; RFC 9982 requires it to stay empty rather than be invented", got)
	}

	// And converting twice must produce identical Cards, which a generated
	// uid would break.
	again, err := FromVcard(c)
	if err != nil {
		t.Fatal(err)
	}
	if card.GetJscontactUid() != again.GetJscontactUid() {
		t.Error("two conversions of one vCard disagree on uid")
	}
}

// TestTitleRoleSplit pins section 2.9.6: two vCard properties, one JSContact
// object keyed on `kind`.
func TestTitleRoleSplit(t *testing.T) {
	c := &vcardv1.Contact{
		DisplayNames: []string{"Test"},
		Titles:       []string{"Head of Research"},
		Roles:        []*vcardv1.Role{{Value: "Project lead"}},
	}
	card, err := FromVcard(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(card.GetTitles()) != 2 {
		t.Fatalf("got %d titles, want both TITLE and ROLE merged in", len(card.GetTitles()))
	}
	kinds := map[cardv1.TitleKind]string{}
	for _, v := range card.GetTitles() {
		kinds[v.GetKind()] = v.GetDisplayName()
	}
	if kinds[cardv1.TitleKind_TITLE_KIND_TITLE] != "Head of Research" {
		t.Errorf("TITLE did not map to kind=title: %v", kinds)
	}
	if kinds[cardv1.TitleKind_TITLE_KIND_ROLE] != "Project lead" {
		t.Errorf("ROLE did not map to kind=role: %v", kinds)
	}

	// And they split back into the two vCard properties.
	back, err := ToVcard(card)
	if err != nil {
		t.Fatal(err)
	}
	if got := back.GetTitles(); len(got) != 1 || got[0] != "Head of Research" {
		t.Errorf("TITLE after round trip = %q", got)
	}
	if got := back.GetRoles(); len(got) != 1 || got[0].GetValue() != "Project lead" {
		t.Errorf("ROLE after round trip = %v", got)
	}
}

// TestPartialBirthdaySurvives pins that a year-less birthday stays year-less.
//
// This is the case google.type.Date exists for and google.protobuf.Timestamp
// cannot express: "14 February, year unknown" is extremely common in address
// books, and inventing a year would fabricate information.
func TestPartialBirthdaySurvives(t *testing.T) {
	c := &vcardv1.Contact{
		DisplayNames: []string{"Test"},
		Birthday: &vcardv1.DateOrText{
			Value: &vcardv1.DateOrText_Date{Date: &date.Date{Month: 2, Day: 14}},
		},
	}
	card, err := FromVcard(c)
	if err != nil {
		t.Fatal(err)
	}
	var got *date.Date
	for _, a := range card.GetAnniversaries() {
		if a.GetKind() == cardv1.AnniversaryKind_ANNIVERSARY_KIND_BIRTH {
			got = a.GetDateValue().GetPartialDate()
		}
	}
	if got == nil {
		t.Fatal("birthday did not convert")
	}
	if got.GetYear() != 0 || got.GetMonth() != 2 || got.GetDay() != 14 {
		t.Errorf("birthday = %v, want month 2 day 14 with no year", got)
	}

	back, err := ToVcard(card)
	if err != nil {
		t.Fatal(err)
	}
	if d := back.GetBirthday().GetDate(); d.GetYear() != 0 || d.GetMonth() != 2 {
		t.Errorf("birthday after round trip = %v; a year was invented", d)
	}
}

// TestConversionIsDeterministic guards the bug this repository already
// shipped once, in the VAVAILABILITY codec: Go randomizes map iteration, and
// JSContact stores most properties in maps, so an unsorted walk would make
// the vCard direction non-deterministic.
func TestConversionIsDeterministic(t *testing.T) {
	card := &cardv1.Card{
		NameComponents: &cardv1.Name{DisplayName: "Test"},
		Emails: map[string]*cardv1.EmailAddress{
			"e1": {Address: "a@example.com"},
			"e2": {Address: "b@example.com"},
			"e3": {Address: "c@example.com"},
			"e4": {Address: "d@example.com"},
		},
	}
	var first []string
	for i := 0; i < 20; i++ {
		c, err := ToVcard(card)
		if err != nil {
			t.Fatal(err)
		}
		order := make([]string, 0, len(c.GetEmails()))
		for _, e := range c.GetEmails() {
			order = append(order, e.GetValue())
		}
		if i == 0 {
			first = order
			continue
		}
		for j := range order {
			if order[j] != first[j] {
				t.Fatalf("email order changed between runs: %v then %v", first, order)
			}
		}
	}
}
