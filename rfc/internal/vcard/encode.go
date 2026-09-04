// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"fmt"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
	"sort"
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
)

// Encode serializes a Contact as a text/vcard object.
//
// Property order follows RFC 6350's own section order. The RFC does not
// require any order, but a deterministic one makes output diffable, which
// matters more than it sounds: without it every test failure is drowned in
// reordering noise.
func Encode(c *vcardv1.Contact) (string, error) {
	lines, err := contentLines(c)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, l := range lines {
		b.WriteString(contentline.Fold(l))
		b.WriteString("\r\n")
	}
	return b.String(), nil
}

// contentLines is the semantic half of encoding: a Contact to unfolded
// content lines. Folding and line breaks are text/vcard's problem, applied by
// Encode; jCard reparses these into properties instead.
func contentLines(c *vcardv1.Contact) ([]string, error) {
	if len(c.GetDisplayNames()) == 0 {
		return nil, fmt.Errorf("contact has no display_names; section 6.2.1 requires FN")
	}

	var out []string
	write := func(l string) { out = append(out, l) }

	write("BEGIN:VCARD")
	// Section 6.7.9 requires VERSION immediately after BEGIN.
	write("VERSION:4.0")

	if k := encodeKind(c.GetKind()); k != "" {
		write("KIND:" + k)
	}
	for _, fn := range c.GetDisplayNames() {
		write("FN:" + contentline.Escape(fn))
	}
	if n := c.GetNameComponents(); n != nil {
		parts := []string{
			contentline.JoinList(n.GetFamilyName()),
			contentline.JoinList(n.GetGivenName()),
			contentline.JoinList(n.GetMiddleNames()),
			contentline.JoinList(n.GetHonorificPrefixes()),
			contentline.JoinList(n.GetHonorificSuffixes()),
		}
		// RFC 9554 section 2.2's two components are appended only when set.
		// Emitting seven unconditionally would add trailing semicolons to
		// every vCard that predates 9554, changing bytes for no gain.
		if len(n.GetSecondarySurnames()) > 0 || len(n.GetGenerations()) > 0 {
			parts = append(parts,
				contentline.JoinList(n.GetSecondarySurnames()),
				contentline.JoinList(n.GetGenerations()),
			)
		}
		write("N:" + strings.Join(parts, ";"))
	}
	for _, e := range c.GetEmails() {
		write("EMAIL" + params(e.GetTypes(), nil, e.GetPref()) + ":" + contentline.Escape(e.GetValue()))
	}
	for _, t := range c.GetTelephones() {
		write("TEL" + params(t.GetTypes(), t.GetFeatures(), t.GetPref()) + ":" + encodeTelValue(t))
	}
	for _, a := range c.GetAddresses() {
		parts := []string{
			contentline.JoinList(a.GetPostOfficeBox()),
			contentline.JoinList(a.GetExtendedAddresses()),
			contentline.JoinList(a.GetStreetAddresses()),
			contentline.JoinList(a.GetLocalities()),
			contentline.JoinList(a.GetRegions()),
			contentline.JoinList(a.GetPostalCodes()),
			contentline.JoinList(a.GetCountries()),
		}
		// RFC 9554 section 2.1's eleven components, appended as a block only
		// when at least one is set -- same reasoning as N above. They are
		// positional, so the whole block goes or none of it does.
		extended := [][]string{
			a.GetRooms(), a.GetApartments(), a.GetFloors(), a.GetStreetNumbers(),
			a.GetStreets(), a.GetBuildings(), a.GetBlocks(), a.GetSubdistricts(),
			a.GetDistricts(), a.GetLandmarks(), a.GetDirections(),
		}
		for _, e := range extended {
			if len(e) > 0 {
				for _, comp := range extended {
					parts = append(parts, contentline.JoinList(comp))
				}
				break
			}
		}
		p := params(a.GetTypes(), nil, a.GetPref())
		// LABEL, RFC 9554 section 4.5. EscapeParam quotes it, which it will
		// always need: a formatted address contains newlines and commas.
		if lbl := a.GetLabel(); lbl != "" {
			p += ";LABEL=" + contentline.EscapeParam(lbl)
		}
		write("ADR" + p + ":" + strings.Join(parts, ";"))
	}
	for _, o := range c.GetOrganizations() {
		v := append([]string{o.GetValue()}, o.GetUnits()...)
		parts := make([]string, len(v))
		for i, s := range v {
			parts[i] = contentline.Escape(s)
		}
		write("ORG" + params(o.GetTypes(), nil, 0) + ":" + strings.Join(parts, ";"))
	}
	for _, t := range c.GetTitles() {
		write("TITLE:" + contentline.Escape(t))
	}
	for _, n := range c.GetNotes() {
		write("NOTE:" + contentline.Escape(n))
	}
	if b := c.GetBirthday(); b != nil {
		v, p := encodeDateOrText(b)
		write("BDAY" + p + ":" + v)
	}
	if a := c.GetAnniversary(); a != nil {
		v, p := encodeDateOrText(a)
		write("ANNIVERSARY" + p + ":" + v)
	}
	for _, n := range c.GetNicknames() {
		write("NICKNAME" + params(n.GetTypes(), nil, n.GetPref()) + ":" + contentline.JoinList(n.GetValues()))
	}
	for _, u := range c.GetUrls() {
		write("URL" + params(u.GetTypes(), nil, u.GetPref()) + ":" + u.GetValue())
	}
	if cats := c.GetCategories(); len(cats) > 0 {
		write("CATEGORIES:" + contentline.JoinList(cats))
	}
	for _, r := range c.GetRoles() {
		write("ROLE" + params(r.GetTypes(), nil, r.GetPref()) + ":" + contentline.Escape(r.GetValue()))
	}
	for _, im := range c.GetInstantMessages() {
		write("IMPP" + params(im.GetTypes(), nil, im.GetPref()) + ":" + im.GetValue())
	}
	for _, lang := range c.GetLanguages() {
		write("LANG" + params(lang.GetTypes(), nil, lang.GetPref()) + ":" + lang.GetValue())
	}
	for _, g := range c.GetLocations() {
		write("GEO" + params(g.GetTypes(), nil, g.GetPref()) + ":" + g.GetValue())
	}
	for _, tz := range c.GetTimezones() {
		v, vp := encodeTimezone(tz)
		write("TZ" + vp + params(tz.GetTypes(), nil, tz.GetPref()) + ":" + v)
	}
	for _, r := range c.GetRelations() {
		v, vp := encodeRelation(r)
		write("RELATED" + vp + relationParams(r.GetRelationTypes(), r.GetPref()) + ":" + v)
	}
	for _, e := range c.GetExtensions() {
		write(encodeExtension(e))
	}
	write("END:VCARD")
	return out, nil
}

func encodeTelValue(t *vcardv1.Telephone) string {
	if u := t.GetUri(); u != "" {
		// A tel: URI is not escaped: it is a URI, not free text.
		return u
	}
	return contentline.Escape(t.GetText())
}

func encodeExtension(e *vcardv1.ExtensionProperty) string {
	var b strings.Builder
	if g := e.GetGroup(); g != "" {
		b.WriteString(g)
		b.WriteByte('.')
	}
	b.WriteString(e.GetKey())

	// Sorted so output is deterministic; Go map order is not.
	keys := make([]string, 0, len(e.GetParameters()))
	for k := range e.GetParameters() {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString(";" + k + "=" + e.GetParameters()[k])
	}
	b.WriteByte(':')
	b.WriteString(contentline.JoinList(e.GetValues()))
	return b.String()
}

// params renders the TYPE and PREF parameters shared by several properties.
func params(types []vcardv1.Type, feats []vcardv1.Feature, pref int32) string {
	var vs []string
	for _, t := range types {
		if s := encodeType(t); s != "" {
			vs = append(vs, s)
		}
	}
	for _, f := range feats {
		if s := encodeFeature(f); s != "" {
			vs = append(vs, s)
		}
	}
	var b strings.Builder
	if len(vs) > 0 {
		b.WriteString(";TYPE=" + strings.Join(vs, ","))
	}
	// Section 5.3: PREF is 1-100. Zero means unset, not most-preferred.
	if pref > 0 {
		fmt.Fprintf(&b, ";PREF=%d", pref)
	}
	return b.String()
}
