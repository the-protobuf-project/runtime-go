// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package vcard

import (
	"strings"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
)

// KIND, RFC 6350 section 6.1.4. An absent KIND means individual, which is why
// the unspecified case is not an error.
func decodeKind(v string) vcardv1.Kind {
	switch strings.ToLower(v) {
	case "individual":
		return vcardv1.Kind_KIND_INDIVIDUAL
	case "group":
		return vcardv1.Kind_KIND_GROUP
	case "org":
		return vcardv1.Kind_KIND_ORG
	case "location":
		return vcardv1.Kind_KIND_LOCATION
	}
	return vcardv1.Kind_KIND_UNSPECIFIED
}

func encodeKind(k vcardv1.Kind) string {
	switch k {
	case vcardv1.Kind_KIND_INDIVIDUAL:
		return "individual"
	case vcardv1.Kind_KIND_GROUP:
		return "group"
	case vcardv1.Kind_KIND_ORG:
		return "org"
	case vcardv1.Kind_KIND_LOCATION:
		return "location"
	}
	return ""
}

// TEL-specific TYPE values, section 6.4.1. These share the TYPE parameter
// with the work/home values in section 5.6, so a TEL can be
// TYPE="work,cell" and both sets have to be read from the same list.
var features = map[string]vcardv1.Feature{
	"TEXT":      vcardv1.Feature_FEATURE_TEXT,
	"VOICE":     vcardv1.Feature_FEATURE_VOICE,
	"FAX":       vcardv1.Feature_FEATURE_FAX,
	"CELL":      vcardv1.Feature_FEATURE_CELL,
	"VIDEO":     vcardv1.Feature_FEATURE_VIDEO,
	"PAGER":     vcardv1.Feature_FEATURE_PAGER,
	"TEXTPHONE": vcardv1.Feature_FEATURE_TEXTPHONE,
}

func decodeFeature(v string) vcardv1.Feature {
	return features[strings.ToUpper(v)]
}

func encodeFeature(f vcardv1.Feature) string {
	for k, v := range features {
		if v == f {
			return strings.ToLower(k)
		}
	}
	return ""
}

func encodeType(t vcardv1.Type) string {
	switch t {
	case vcardv1.Type_TYPE_WORK:
		return "work"
	case vcardv1.Type_TYPE_HOME:
		return "home"
	// RFC 9554 section 5. Defined for ADR only, but encoded wherever the
	// model carries them: rejecting here would lose a value the schema
	// accepted.
	case vcardv1.Type_TYPE_BILLING:
		return "billing"
	case vcardv1.Type_TYPE_DELIVERY:
		return "delivery"
	}
	return ""
}
