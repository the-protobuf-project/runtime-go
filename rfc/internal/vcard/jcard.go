// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"encoding/json"
	"fmt"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
)

// jCard, RFC 7095: application/vcard+json.
//
// RFC 7095 defines jCard as an alternate encoding of RFC 6350's *data model*,
// not as a different model. This file is therefore only a syntax layer -- it
// converts between JSON and the same content lines text/vcard uses, and the
// semantic mapping in decodeLines and contentLines is shared verbatim. If
// Contact is right for one encoding it is right for both, which is the claim
// worth making structurally rather than in a comment.
//
// Shape, section 3.2:
//
//	["vcard", [ [name, params, type, value...], ... ]]

// structured lists the properties whose value is semicolon-separated
// components, RFC 7095 section 3.3.1.3. jCard writes those as a JSON array;
// every other property is a plain string.
var structured = map[string]bool{"N": true, "ADR": true, "ORG": true}

// EncodeJCard serializes a Contact as application/vcard+json.
func EncodeJCard(c *vcardv1.Contact) ([]byte, error) {
	raws, err := contentLines(c)
	if err != nil {
		return nil, err
	}

	props := []any{}
	for _, raw := range raws {
		l, err := contentline.Parse(raw)
		if err != nil {
			return nil, err
		}
		// BEGIN and END are vCard's line delimiters; jCard's array brackets
		// take their place, and section 3.2 has no properties for them.
		if l.Name == "BEGIN" || l.Name == "END" {
			continue
		}
		props = append(props, jcardProperty(l))
	}
	return json.Marshal([]any{"vcard", props})
}

func jcardProperty(l contentline.Line) []any {
	// Section 3.3: names and parameter names MUST be lowercase.
	name := strings.ToLower(l.RawName)
	if l.Group != "" {
		name = strings.ToLower(l.Group) + "." + name
	}

	params := map[string]any{}
	for k, vs := range l.Params {
		lk := strings.ToLower(k)
		if len(vs) == 1 {
			params[lk] = vs[0]
		} else {
			params[lk] = vs
		}
	}

	jtype := jcardType(l)
	prop := []any{name, params, jtype}

	// Section 5.1: an unknown value type carries "the unprocessed value text"
	// as a single JSON string. Splitting it on commas or unescaping it is
	// precisely the "additional escaping ... that breaks round-tripping" the
	// section warns about -- RFC 7095's own example keeps
	// "Stenophylla;Guinea\\,Africa" intact.
	if jtype == "unknown" {
		return append(prop, l.Value)
	}
	if jtype == "date-and-or-time" || jtype == "date" || jtype == "date-time" {
		return append(prop, basicToExtendedDate(l.Value))
	}

	if structured[l.Name] {
		comps := []any{}
		for _, part := range contentline.SplitUnescaped(l.Value, ';') {
			vals := contentline.SplitList(part)
			switch len(vals) {
			case 0:
				comps = append(comps, "")
			case 1:
				comps = append(comps, vals[0])
			default:
				// A component with several values is itself an array.
				inner := make([]any, len(vals))
				for i, v := range vals {
					inner[i] = v
				}
				comps = append(comps, inner)
			}
		}
		return append(prop, comps)
	}

	// A multi-valued property becomes extra array elements, section 3.3.1.3.
	vals := contentline.SplitList(l.Value)
	if len(vals) == 0 {
		return append(prop, contentline.Unescape(l.Value))
	}
	for _, v := range vals {
		prop = append(prop, v)
	}
	return prop
}

// jcardType is the value-type identifier, section 3.3.1.2. VALUE says so when
// present; otherwise text is the default for every property this schema
// models, except a tel: URI, the uriValued properties (always uri, no text
// alternative in their own ABNF) and LANG (always language-tag).
func jcardType(l contentline.Line) string {
	if v := l.Params["VALUE"]; len(v) > 0 {
		return strings.ToLower(v[0])
	}
	if l.Name == "TEL" && strings.HasPrefix(strings.ToLower(l.Value), "tel:") {
		return "uri"
	}
	if uriValued[l.Name] {
		return "uri"
	}
	if l.Name == "LANG" {
		return "language-tag"
	}
	// Section 5 of RFC 7095: "BDAY's default value type is
	// 'date-and-or-time'". Calling it text also meant the value went out in
	// vCard's basic form, so a birthday read as "19850412" where section 3.5.3
	// wants "1985-04-12".
	if l.Name == "BDAY" || l.Name == "ANNIVERSARY" {
		return "date-and-or-time"
	}
	// Section 5.1: a property with no VALUE parameter whose default value type
	// is not known "MUST be converted to a primitive JSON string ... Also,
	// value type MUST be set to 'unknown'", and section 5 warns that using
	// "text" instead lets "additional escaping ... break round-tripping".
	if !knownDefault[l.Name] {
		return "unknown"
	}
	return "text"
}

// basicToExtendedDate converts a vCard section 4.3 date or date-time to the
// ISO 8601 extended form RFC 7095 section 3.5.3 requires, covering the
// reduced-accuracy forms:
//
//	19850412 -> 1985-04-12    --0412 -> --04-12    ---12 -> ---12
//	1985-04  -> 1985-04       1985   -> 1985
//	20130214T123000 -> 2013-02-14T12:30:00
//
// A value it does not recognize is returned unchanged rather than mangled:
// section 4.3.4 permits forms this schema does not model, and passing one
// through intact is better than emitting a half-converted string.
func basicToExtendedDate(v string) string {
	d, t, hasTime := strings.Cut(v, "T")

	var out string
	switch {
	case strings.HasPrefix(d, "---") && len(d) == 5: // ---DD
		out = d
	case strings.HasPrefix(d, "--") && len(d) == 6: // --MMDD
		out = "--" + d[2:4] + "-" + d[4:6]
	case strings.HasPrefix(d, "--") && len(d) == 4: // --MM
		out = d
	case len(d) == 8 && isDigits(d): // YYYYMMDD
		out = d[0:4] + "-" + d[4:6] + "-" + d[6:8]
	case len(d) == 7 && d[4] == '-': // YYYY-MM
		out = d
	case len(d) == 4 && isDigits(d): // YYYY
		out = d
	case d == "":
		out = ""
	default:
		return v
	}
	if !hasTime {
		return out
	}

	zone := ""
	for _, suffix := range []string{"Z", "+", "-"} {
		if i := strings.LastIndex(t, suffix); i > 0 {
			zone, t = t[i:], t[:i]
			break
		}
	}
	if strings.HasSuffix(t, "Z") {
		zone, t = "Z", strings.TrimSuffix(t, "Z")
	}
	switch {
	case len(t) == 6 && isDigits(t):
		t = t[0:2] + ":" + t[2:4] + ":" + t[4:6]
	case len(t) == 4 && isDigits(t):
		t = t[0:2] + ":" + t[2:4]
	case len(t) == 2 && isDigits(t):
	default:
		return v
	}
	return out + "T" + t + zone
}

func isDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}

// DecodeJCard parses application/vcard+json into a Contact.
func DecodeJCard(data []byte) (*vcardv1.Contact, error) {
	var doc []any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("jcard is not JSON: %w", err)
	}
	if len(doc) != 2 {
		return nil, fmt.Errorf("jcard must be a 2-element array, got %d", len(doc))
	}
	if s, _ := doc[0].(string); s != "vcard" {
		return nil, fmt.Errorf("jcard first element is %v, want \"vcard\"", doc[0])
	}
	rawProps, ok := doc[1].([]any)
	if !ok {
		return nil, fmt.Errorf("jcard second element is not an array")
	}

	// decodeLines expects the delimiters that jCard's brackets replace.
	lines := []contentline.Line{{Name: "BEGIN", Value: "VCARD", Params: map[string][]string{}}}
	for _, rp := range rawProps {
		l, err := jcardToLine(rp)
		if err != nil {
			return nil, err
		}
		lines = append(lines, l)
	}
	lines = append(lines, contentline.Line{Name: "END", Value: "VCARD", Params: map[string][]string{}})
	return decodeLines(lines)
}
