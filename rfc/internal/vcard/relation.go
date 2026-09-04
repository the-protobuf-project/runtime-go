// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"fmt"
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/contentline"
)

// relationParams renders RELATED's TYPE and PREF parameters. A standalone
// function, not params(), because RELATED's TYPE values come from
// RelationType, not the generic work/home Type every other property uses.
func relationParams(types []vcardv1.RelationType, pref int32) string {
	var vs []string
	for _, t := range types {
		if s := encodeRelationType(t); s != "" {
			vs = append(vs, s)
		}
	}
	var b strings.Builder
	if len(vs) > 0 {
		b.WriteString(";TYPE=" + strings.Join(vs, ","))
	}
	if pref > 0 {
		fmt.Fprintf(&b, ";PREF=%d", pref)
	}
	return b.String()
}

// Codec for Relation, RELATED's value, section 6.6.6. Unlike TZ, the default
// here is uri, not text -- section 6.6.6's own examples write a bare URI
// with no VALUE parameter at all, and reserve VALUE=text for free text.

// encodeRelation renders the value and the VALUE parameter its form needs.
func encodeRelation(r *vcardv1.Relation) (value, valueParam string) {
	switch v := r.GetValue().(type) {
	case *vcardv1.Relation_Uri:
		// Section 6.6.6's default; no parameter needed.
		return v.Uri, ""
	case *vcardv1.Relation_Text:
		return contentline.Escape(v.Text), ";VALUE=text"
	}
	return "", ""
}

func decodeRelation(l contentline.Line) *vcardv1.Relation {
	r := &vcardv1.Relation{RelationTypes: decodeRelationTypes(l), Pref: decodePref(l)}
	// Not valueType(l): that helper defaults to "text" when VALUE is absent,
	// which is right for every other property in this package but wrong
	// here -- section 6.6.6's own default is uri, so only an explicit
	// VALUE=text switches branches.
	if v := l.Params["VALUE"]; len(v) > 0 && strings.EqualFold(v[0], "text") {
		r.Value = &vcardv1.Relation_Text{Text: contentline.Unescape(l.Value)}
	} else {
		r.Value = &vcardv1.Relation_Uri{Uri: l.Value}
	}
	return r
}

// relationTypes is RELATED's own TYPE value set, section 6.6.6, disjoint
// from the generic work/home Type every other property in this package uses.
var relationTypes = map[string]vcardv1.RelationType{
	"CONTACT":      vcardv1.RelationType_RELATION_TYPE_CONTACT,
	"ACQUAINTANCE": vcardv1.RelationType_RELATION_TYPE_ACQUAINTANCE,
	"FRIEND":       vcardv1.RelationType_RELATION_TYPE_FRIEND,
	"MET":          vcardv1.RelationType_RELATION_TYPE_MET,
	"CO-WORKER":    vcardv1.RelationType_RELATION_TYPE_CO_WORKER,
	"COLLEAGUE":    vcardv1.RelationType_RELATION_TYPE_COLLEAGUE,
	"CO-RESIDENT":  vcardv1.RelationType_RELATION_TYPE_CO_RESIDENT,
	"NEIGHBOR":     vcardv1.RelationType_RELATION_TYPE_NEIGHBOR,
	"CHILD":        vcardv1.RelationType_RELATION_TYPE_CHILD,
	"PARENT":       vcardv1.RelationType_RELATION_TYPE_PARENT,
	"SIBLING":      vcardv1.RelationType_RELATION_TYPE_SIBLING,
	"SPOUSE":       vcardv1.RelationType_RELATION_TYPE_SPOUSE,
	"KIN":          vcardv1.RelationType_RELATION_TYPE_KIN,
	"MUSE":         vcardv1.RelationType_RELATION_TYPE_MUSE,
	"CRUSH":        vcardv1.RelationType_RELATION_TYPE_CRUSH,
	"DATE":         vcardv1.RelationType_RELATION_TYPE_DATE,
	"SWEETHEART":   vcardv1.RelationType_RELATION_TYPE_SWEETHEART,
	"ME":           vcardv1.RelationType_RELATION_TYPE_ME,
	"AGENT":        vcardv1.RelationType_RELATION_TYPE_AGENT,
	"EMERGENCY":    vcardv1.RelationType_RELATION_TYPE_EMERGENCY,
}

func decodeRelationTypes(l contentline.Line) []vcardv1.RelationType {
	var out []vcardv1.RelationType
	for _, v := range l.Params["TYPE"] {
		if rt, ok := relationTypes[strings.ToUpper(v)]; ok {
			out = append(out, rt)
		}
	}
	return out
}

func encodeRelationType(t vcardv1.RelationType) string {
	for k, v := range relationTypes {
		if v == t {
			return strings.ToLower(k)
		}
	}
	return ""
}
