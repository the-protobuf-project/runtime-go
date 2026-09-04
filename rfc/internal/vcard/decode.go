// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"fmt"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
)

// Decode parses one text/vcard object into a Contact.
//
// Properties this schema models become typed fields. Everything else becomes
// an ExtensionProperty, which is what makes a lossless round trip possible --
// see RFC 6350 section 6.10.
func Decode(src string) (*vcardv1.Contact, error) {
	raws := contentline.Unfold(src)
	if len(raws) == 0 {
		return nil, fmt.Errorf("empty vcard")
	}
	lines := make([]contentline.Line, 0, len(raws))
	for _, raw := range raws {
		l, err := contentline.Parse(raw)
		if err != nil {
			return nil, err
		}
		lines = append(lines, l)
	}
	return decodeLines(lines)
}

// decodeLines is the semantic half of decoding: content lines to a Contact,
// with no syntax left in it.
//
// Both codecs end here. text/vcard reaches it through parseLine and jCard
// through jcardToLines, which is what makes "one data model, several
// encodings" true of the implementation and not just of the RFCs.
func decodeLines(lines []contentline.Line) (*vcardv1.Contact, error) {
	c := &vcardv1.Contact{}
	seenBegin, seenVersion := false, false

	for _, l := range lines {
		switch l.Name {
		case "BEGIN":
			if !strings.EqualFold(l.Value, "VCARD") {
				return nil, fmt.Errorf("BEGIN is %q, want VCARD", l.Value)
			}
			seenBegin = true
			continue
		case "END":
			continue
		case "VERSION":
			// Section 6.7.9: MUST be 4.0 and MUST come directly after BEGIN.
			if l.Value != "4.0" {
				return nil, fmt.Errorf("VERSION is %q, want 4.0", l.Value)
			}
			seenVersion = true
			continue
		}
		if !seenBegin {
			return nil, fmt.Errorf("content line before BEGIN:VCARD: %s", l.Name)
		}
		decodeProperty(c, l)
	}

	if !seenBegin {
		return nil, fmt.Errorf("missing BEGIN:VCARD")
	}
	if !seenVersion {
		return nil, fmt.Errorf("missing VERSION:4.0")
	}
	// Section 6.2.1 makes FN the only mandatory property.
	if len(c.GetDisplayNames()) == 0 {
		return nil, fmt.Errorf("missing FN, which section 6.2.1 requires")
	}
	return c, nil
}

func decodeProperty(c *vcardv1.Contact, l contentline.Line) {
	switch l.Name {
	case "FN":
		c.DisplayNames = append(c.DisplayNames, contentline.Unescape(l.Value))
	case "N":
		c.NameComponents = decodeName(l.Value)
	case "KIND":
		c.Kind = decodeKind(l.Value)
	case "EMAIL":
		c.Emails = append(c.Emails, &vcardv1.Email{
			Value: contentline.Unescape(l.Value),
			Types: decodeTypes(l),
			Pref:  decodePref(l),
		})
	case "TEL":
		c.Telephones = append(c.Telephones, decodeTelephone(l))
	case "ADR":
		c.Addresses = append(c.Addresses, decodeAddress(l))
	case "ORG":
		c.Organizations = append(c.Organizations, decodeOrganization(l))
	case "TITLE":
		c.Titles = append(c.Titles, contentline.Unescape(l.Value))
	case "NOTE":
		c.Notes = append(c.Notes, contentline.Unescape(l.Value))
	case "BDAY":
		c.Birthday = decodeDateOrText(l)
	case "ANNIVERSARY":
		c.Anniversary = decodeDateOrText(l)
	case "NICKNAME":
		c.Nicknames = append(c.Nicknames, decodeNickname(l))
	case "URL":
		c.Urls = append(c.Urls, decodeUrl(l))
	case "CATEGORIES":
		c.Categories = append(c.Categories, contentline.SplitList(l.Value)...)
	case "ROLE":
		c.Roles = append(c.Roles, decodeRole(l))
	case "IMPP":
		c.InstantMessages = append(c.InstantMessages, decodeInstantMessage(l))
	case "LANG":
		c.Languages = append(c.Languages, decodeLanguage(l))
	case "GEO":
		c.Locations = append(c.Locations, decodeGeo(l))
	case "TZ":
		c.Timezones = append(c.Timezones, decodeTimezone(l))
	case "RELATED":
		c.Relations = append(c.Relations, decodeRelation(l))
	default:
		c.Extensions = append(c.Extensions, extensionOf(l))
	}
}

// extensionOf preserves a property this schema does not model. Section 6.10
// and the iana-token / x-name production in section 3.3 both require that a
// conforming implementation carry these rather than drop them.
func extensionOf(l contentline.Line) *vcardv1.ExtensionProperty {
	e := &vcardv1.ExtensionProperty{
		Key:    l.RawName,
		Group:  l.Group,
		Values: contentline.SplitList(l.Value),
	}
	if len(e.Values) == 0 && l.Value != "" {
		e.Values = []string{contentline.Unescape(l.Value)}
	}
	if len(l.Params) > 0 {
		e.Parameters = map[string]string{}
		for k, v := range l.Params {
			e.Parameters[k] = strings.Join(v, ",")
		}
	}
	return e
}
