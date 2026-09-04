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

	prop := []any{name, params, jcardType(l)}

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
	return "text"
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
