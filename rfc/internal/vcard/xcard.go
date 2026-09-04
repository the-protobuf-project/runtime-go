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
			// Each parameter wraps its values in a type element,
			// section 3.3: <type><text>work</text></type>.
			pn := node{XMLName: xml.Name{Local: strings.ToLower(k)}}
			for _, v := range l.Params[k] {
				pn.Children = append(pn.Children, node{
					XMLName:  xml.Name{Local: "text"},
					Chardata: v,
				})
			}
			params.Children = append(params.Children, pn)
		}
		p.Children = append(p.Children, params)
	}

	valueType := xcardValueType(l)

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
	if ns := doc.XMLName.Space; ns != "" && ns != Namespace {
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
