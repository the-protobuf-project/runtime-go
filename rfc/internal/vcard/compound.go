// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
	"strconv"
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
)

// Decoders for RFC 6350's compound properties -- N, ADR and ORG -- whose
// values are semicolon-separated component lists, each component itself
// comma-separated. Sections 6.2.2, 6.3.1 and 6.6.4.

func decodeName(v string) *vcardv1.Name {
	p := contentline.SplitUnescaped(v, ';')
	get := func(i int) []string {
		if i >= len(p) {
			return nil
		}
		return contentline.SplitList(p[i])
	}
	// Components 5 and 6 are RFC 9554 section 2.2. get() returns nil past the
	// end, so a five-component RFC 6350 N decodes unchanged.
	return &vcardv1.Name{
		FamilyName:        get(0),
		GivenName:         get(1),
		MiddleNames:       get(2),
		HonorificPrefixes: get(3),
		HonorificSuffixes: get(4),
		SecondarySurnames: get(5),
		Generations:       get(6),
	}
}

func decodeAddress(l contentline.Line) *vcardv1.Address {
	p := contentline.SplitUnescaped(l.Value, ';')
	get := func(i int) []string {
		if i >= len(p) {
			return nil
		}
		return contentline.SplitList(p[i])
	}
	// Components 7 to 17 are RFC 9554 section 2.1. Before this they were
	// parsed and thrown away: SplitUnescaped produced them and nothing read
	// them, so an 18-component ADR lost everything past the country.
	return &vcardv1.Address{
		PostOfficeBox:     get(0),
		ExtendedAddresses: get(1),
		StreetAddresses:   get(2),
		Localities:        get(3),
		Regions:           get(4),
		PostalCodes:       get(5),
		Countries:         get(6),
		Rooms:             get(7),
		Apartments:        get(8),
		Floors:            get(9),
		StreetNumbers:     get(10),
		Streets:           get(11),
		Buildings:         get(12),
		Blocks:            get(13),
		Subdistricts:      get(14),
		Districts:         get(15),
		Landmarks:         get(16),
		Directions:        get(17),
		Types:             decodeTypes(l),
		Pref:              decodePref(l),
		// LABEL, RFC 9554 section 4.5. Safe to map only since the parser
		// stopped splitting inside a quoted-string: a formatted address is
		// free text and "1 Main St, Springfield" is one value.
		Label: firstParam(l, "LABEL"),
	}
}

func decodeOrganization(l contentline.Line) *vcardv1.Organization {
	p := contentline.SplitCompound(l.Value)
	o := &vcardv1.Organization{Types: decodeTypes(l)}
	if len(p) > 0 {
		o.Value = p[0]
	}
	if len(p) > 1 {
		o.Units = p[1:]
	}
	return o
}

func decodeTelephone(l contentline.Line) *vcardv1.Telephone {
	t := &vcardv1.Telephone{Types: decodeTypes(l), Pref: decodePref(l)}
	// Section 6.4.1: the default value type is text, but the value SHOULD be
	// a tel: URI. Both occur, and the oneof keeps them distinct.
	if strings.HasPrefix(strings.ToLower(l.Value), "tel:") {
		t.Value = &vcardv1.Telephone_Uri{Uri: l.Value}
	} else {
		t.Value = &vcardv1.Telephone_Text{Text: contentline.Unescape(l.Value)}
	}
	for _, f := range l.Params["TYPE"] {
		if feat := decodeFeature(f); feat != vcardv1.Feature_FEATURE_UNSPECIFIED {
			t.Features = append(t.Features, feat)
		}
	}
	return t
}

func decodePref(l contentline.Line) int32 {
	if v := l.Params["PREF"]; len(v) > 0 {
		if n, err := strconv.ParseInt(v[0], 10, 32); err == nil {
			return int32(n)
		}
	}
	return 0
}

func decodeTypes(l contentline.Line) []vcardv1.Type {
	var out []vcardv1.Type
	for _, v := range l.Params["TYPE"] {
		switch strings.ToUpper(v) {
		case "WORK":
			out = append(out, vcardv1.Type_TYPE_WORK)
		case "HOME":
			out = append(out, vcardv1.Type_TYPE_HOME)
		// RFC 9554 section 5.
		case "BILLING":
			out = append(out, vcardv1.Type_TYPE_BILLING)
		case "DELIVERY":
			out = append(out, vcardv1.Type_TYPE_DELIVERY)
		}
	}
	return out
}

// firstParam returns a parameter's first value, or "" when it is absent.
func firstParam(l contentline.Line, name string) string {
	if v := l.Params[name]; len(v) > 0 {
		return contentline.Unescape(v[0])
	}
	return ""
}
