// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"encoding/xml"
	"fmt"
	"sort"
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
)

// EncodeXCard serializes a Contact as application/vcard+xml.
func EncodeXCard(c *vcardv1.Contact) ([]byte, error) {
	raws, err := contentLines(c)
	if err != nil {
		return nil, err
	}

	var props []node
	// Grouped properties share one <group name="..."> wrapper, section 3.2.
	groups := map[string]int{}

	for _, raw := range raws {
		l, parseErr := contentline.Parse(raw)
		if parseErr != nil {
			return nil, parseErr
		}
		// BEGIN and END are vCard's delimiters; <vcard> replaces them.
		// VERSION is absent by design: the namespace carries the version.
		if l.Name == "BEGIN" || l.Name == "END" || l.Name == "VERSION" {
			continue
		}
		p := xcardProperty(l)
		if l.Group == "" {
			props = append(props, p)
			continue
		}
		idx, seen := groups[l.Group]
		if !seen {
			props = append(props, node{
				XMLName: xml.Name{Local: "group"},
				Attrs:   []xml.Attr{{Name: xml.Name{Local: "name"}, Value: l.Group}},
			})
			idx = len(props) - 1
			groups[l.Group] = idx
		}
		props[idx].Children = append(props[idx].Children, p)
	}

	// The namespace is declared as an attribute rather than through
	// XMLName.Space: setting Space on the root alone makes encoding/xml stamp
	// xmlns="" on every child, which un-declares the namespace for exactly
	// the elements that need it.
	doc := node{
		XMLName:  xml.Name{Local: "vcards"},
		Attrs:    []xml.Attr{{Name: xml.Name{Local: "xmlns"}, Value: Namespace}},
		Children: []node{{XMLName: xml.Name{Local: "vcard"}, Children: props}},
	}
	out, err := xml.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, err
	}
	return append([]byte(xml.Header), out...), nil
}

func xcardProperty(l contentline.Line) node {
	p := node{XMLName: xml.Name{Local: strings.ToLower(l.RawName)}}

	if len(l.Params) > 0 {
		params := node{XMLName: xml.Name{Local: "parameters"}}
		// Sorted so output is deterministic; Go map order is not.
		keys := make([]string, 0, len(l.Params))
		for k := range l.Params {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			// Each parameter wraps its values in a value element,
			// section 3.3: <type><text>work</text></type>. Which element
			// depends on the parameter -- section 5's schema gives PREF an
			// <integer> and LANGUAGE a <language-tag>, and writing <text> for
			// every one produced a document that fails the RELAX NG schema
			// RFC 6351 appendix A defines.
			name := strings.ToLower(k)
			pn := node{XMLName: xml.Name{Local: name}}
			for _, v := range l.Params[k] {
				pn.Children = append(pn.Children, node{
					XMLName:  xml.Name{Local: xcardParamValueType(name)},
					Chardata: v,
				})
			}
			params.Children = append(params.Children, pn)
		}
		p.Children = append(p.Children, params)
	}

	valueType := xcardValueType(l)

	// An <unknown> value carries the unprocessed value text, so it is neither
	// split nor unescaped.
	if valueType == "unknown" {
		p.Children = append(p.Children, node{
			XMLName:  xml.Name{Local: "unknown"},
			Chardata: l.Value,
		})
		return p
	}

	if names, ok := namedComponents[l.Name]; ok {
		for i, part := range contentline.SplitUnescaped(l.Value, ';') {
			if i >= len(names) {
				break
			}
			// A component with several values repeats its element.
			vals := contentline.SplitList(part)
			if len(vals) == 0 {
				p.Children = append(p.Children, node{XMLName: xml.Name{Local: names[i]}})
				continue
			}
			for _, v := range vals {
				p.Children = append(p.Children, node{
					XMLName:  xml.Name{Local: names[i]},
					Chardata: v,
				})
			}
		}
		return p
	}

	sep := byte(',')
	if semicolonJoined[l.Name] {
		sep = ';'
	}
	for _, part := range contentline.SplitUnescaped(l.Value, sep) {
		p.Children = append(p.Children, node{
			XMLName:  xml.Name{Local: valueType},
			Chardata: contentline.Unescape(part),
		})
	}
	return p
}

// xcardParamValueType is the value element a parameter's values are wrapped
// in, from section 5's schema:
//
//	5.2  param-pref     = element pref { element integer { 1..100 } }
//	5.1  param-language = element language { value-language-tag }
//	5.10 param-geo      = element geo { value-uri }
//	5.11 param-tz       = element tz { value-text | value-uri }
//
// Everything else -- ALTID, PID, TYPE, MEDIATYPE, CALSCALE, SORT-AS -- takes
// value-text. TZ is given <text>, which its schema permits either way, since
// a TZ parameter is as often a zone name as a URI.
func xcardParamValueType(name string) string {
	switch name {
	case "pref":
		return "integer"
	case "language":
		return "language-tag"
	case "geo":
		return "uri"
	}
	return "text"
}

// xcardValueType is the element a value is wrapped in, section 3.3. Mirrors
// jcardType's defaulting exactly; the two encodings share the same VALUE
// semantics, just a different container.
func xcardValueType(l contentline.Line) string {
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
	// Section 4: "Any property that does not include a 'VALUE' parameter and
	// whose default value type is not known MUST be converted using the value
	// type XML element <unknown>. The content of that element is the
	// unprocessed value text." The same rule RFC 7095 section 5.1 states for
	// jCard, and for the same reason -- <text> would re-escape the value.
	if !knownDefault[l.Name] {
		return "unknown"
	}
	return "text"
}

// DecodeXCard parses application/vcard+xml into a Contact.
func DecodeXCard(data []byte) (*vcardv1.Contact, error) {
	var doc node
	if err := xml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("xcard is not XML: %w", err)
	}
	if doc.XMLName.Local != "vcards" {
		return nil, fmt.Errorf("xcard root is <%s>, want <vcards>", doc.XMLName.Local)
	}
	// Section 3.1 puts every xCard element in this namespace, and it is what
	// stands in for VERSION:4.0 below -- so an unnamespaced <vcards> is not a
	// lenient case to wave through, it is a document with no version at all.
	if ns := doc.XMLName.Space; ns != Namespace {
		return nil, fmt.Errorf("xcard namespace is %q, want %q", ns, Namespace)
	}
	card, ok := doc.child("vcard")
	if !ok {
		return nil, fmt.Errorf("xcard has no <vcard> element")
	}

	// The namespace stands in for VERSION, and <vcard> for BEGIN and END, so
	// the shared decoder is given the delimiters it requires.
	lines := []contentline.Line{
		{Name: "BEGIN", Value: "VCARD", Params: map[string][]string{}},
		{Name: "VERSION", Value: "4.0", Params: map[string][]string{}},
	}
	for _, el := range card.Children {
		if el.XMLName.Local == "group" {
			g := el.attr("name")
			for _, inner := range el.Children {
				l := xcardToLine(inner)
				l.Group = g
				lines = append(lines, l)
			}
			continue
		}
		l := xcardToLine(el)
		lines = append(lines, l)
	}
	lines = append(lines, contentline.Line{Name: "END", Value: "VCARD", Params: map[string][]string{}})
	return decodeLines(lines)
}
