// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"strings"

	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
)

// xcardToLine converts one xCard property element into a content line, so the
// shared semantic decoder consumes it unchanged.
//
// It cannot fail: the XML is already parsed by the time this runs, so every
// element it walks is well-formed by construction, and an element it does not
// recognize becomes a line the semantic decoder ignores rather than an error.
func xcardToLine(el node) contentline.Line {
	name := el.XMLName.Local
	l := contentline.Line{
		RawName: name,
		Name:    strings.ToUpper(name),
		Params:  map[string][]string{},
	}

	var valueChildren []node
	for _, c := range el.Children {
		if c.XMLName.Local == "parameters" {
			for _, p := range c.Children {
				key := strings.ToUpper(p.XMLName.Local)
				// Each parameter value is wrapped in a type element, so the
				// text is one level below the parameter name.
				for _, v := range p.Children {
					l.Params[key] = append(l.Params[key], strings.TrimSpace(v.Chardata))
				}
				if len(p.Children) == 0 {
					if s := strings.TrimSpace(p.Chardata); s != "" {
						l.Params[key] = append(l.Params[key], s)
					}
				}
			}
			continue
		}
		valueChildren = append(valueChildren, c)
	}

	// Section 4: "Any <unknown> property value XML elements are converted
	// directly into vCard values. The containing property MUST NOT have a
	// 'VALUE' parameter." So the text is taken through unprocessed and the
	// element name is not recorded -- <unknown> is xCard's own marker and has
	// no vCard VALUE spelling.
	if len(valueChildren) == 1 && valueChildren[0].XMLName.Local == "unknown" {
		l.Value = valueChildren[0].Chardata
		return l
	}

	// The value-type element is recorded as VALUE only when it is not text,
	// so a round trip does not invent VALUE=text on every property.
	if len(valueChildren) > 0 {
		if t := valueChildren[0].XMLName.Local; t != "text" {
			if _, named := namedComponents[l.Name]; !named {
				if _, seen := l.Params["VALUE"]; !seen {
					l.Params["VALUE"] = []string{t}
				}
			}
		}
	}

	if names, ok := namedComponents[l.Name]; ok {
		l.Value = xcardNamedValue(names, valueChildren)
		return l
	}

	sep := ","
	if semicolonJoined[l.Name] {
		sep = ";"
	}
	parts := make([]string, 0, len(valueChildren))
	for _, c := range valueChildren {
		// A <uri>, <language-tag> or <utc-offset> value is never escaped.
		// Section 3.4 of RFC 6350 escapes TEXT values only, and section
		// 6.4.1's own example writes "tel:+1-555-555-5555;ext=5555" with a
		// bare semicolon -- escaping it would change the URI. Same rule as
		// jCard, found the same way.
		if unescapedTypes[c.XMLName.Local] {
			parts = append(parts, c.Chardata)
			continue
		}
		parts = append(parts, contentline.Escape(c.Chardata))
	}
	l.Value = strings.Join(parts, sep)
	return l
}

// xcardNamedValue rebuilds a semicolon-separated compound value from named
// child elements, RFC 6351 section 3.3.
//
// Order comes from the name table, not from document order: XML is free to
// present the elements in any order, and reading them positionally would
// scramble a card whose exporter wrote <given> before <surname>. A component
// repeated across several elements is comma-joined, which is how a
// multi-valued component such as N's suffix is carried.
func xcardNamedValue(names []string, children []node) string {
	byName := map[string][]string{}
	for _, c := range children {
		n := c.XMLName.Local
		byName[n] = append(byName[n], contentline.Escape(c.Chardata))
	}
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = strings.Join(byName[n], ",")
	}
	return strings.Join(parts, ";")
}
