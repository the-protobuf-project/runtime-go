// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import "encoding/xml"

// xCard, RFC 6351: application/vcard+xml.
//
// The third encoding of RFC 6350's data model, and the one that reshapes it
// most. Where jCard writes a structured value as a positional array, xCard
// uses *named* child elements -- <surname>, <given> -- so the mapping needs a
// component-name table per property rather than an index.
//
// As with jCard, only the syntax differs: decodeLines and contentLines are
// shared with text/vcard unchanged.

// Namespace is the xCard namespace, RFC 6351 section 4. It carries the vCard
// version, which is why xCard has no VERSION property at all.
const Namespace = "urn:ietf:params:xml:ns:vcard-4.0"

// node is a generic XML element, so the codec can read any property without
// a Go type per property.
type node struct {
	XMLName  xml.Name
	Attrs    []xml.Attr `xml:",any,attr"`
	Chardata string     `xml:",chardata"`
	Children []node     `xml:",any"`
}

// namedComponents lists the properties whose structured value uses named
// child elements, RFC 6351 section 3.3, and the order those names appear in
// the equivalent text/vcard value.
var namedComponents = map[string][]string{
	"N":   {"surname", "given", "additional", "prefix", "suffix"},
	"ADR": {"pobox", "ext", "street", "locality", "region", "code", "country"},
}

// semicolonJoined lists properties whose components are semicolon-separated
// in text/vcard. ORG is structured but its components are unnamed: xCard
// writes them as repeated <text>, which looks identical to a multi-valued
// property like CATEGORIES and is joined differently.
var semicolonJoined = map[string]bool{"N": true, "ADR": true, "ORG": true}

// child returns the first child element with the given local name.
func (n node) child(name string) (node, bool) {
	for _, c := range n.Children {
		if c.XMLName.Local == name {
			return c, true
		}
	}
	return node{}, false
}

// attr returns an attribute value by local name.
func (n node) attr(name string) string {
	for _, a := range n.Attrs {
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}
