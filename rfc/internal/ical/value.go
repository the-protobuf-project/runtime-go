// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package ical

import (
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/genproto/googleapis/type/latlng"

	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc5545/event/v1"
)

// decodeGeo reads GEO, section 3.8.1.6: two semicolon-separated floats.
//
// Unlike vCard's GEO, which section 6.5.2 types as a URI, this really is a
// coordinate pair, which is why only this one maps to google.type.LatLng.
func decodeGeo(v string) (*latlng.LatLng, error) {
	parts := strings.Split(strings.TrimSpace(v), ";")
	if len(parts) != 2 {
		return nil, fmt.Errorf("GEO %q is not lat;lon", v)
	}
	lat, err1 := strconv.ParseFloat(parts[0], 64)
	lon, err2 := strconv.ParseFloat(parts[1], 64)
	if err1 != nil || err2 != nil {
		return nil, fmt.Errorf("GEO %q is not numeric", v)
	}
	return &latlng.LatLng{Latitude: lat, Longitude: lon}, nil
}

func encodeGeo(p *latlng.LatLng) string {
	return strconv.FormatFloat(p.GetLatitude(), 'f', -1, 64) + ";" +
		strconv.FormatFloat(p.GetLongitude(), 'f', -1, 64)
}

// STATUS for a VEVENT, section 3.8.1.11.
//
// CANCELLED is spelled with two Ls throughout because that is the wire value
// RFC 5545 registers and the name protoc-gen-go derives from the enum. It is
// not a spelling this package may choose.
//
//nolint:misspell // RFC 5545 section 3.8.1.11 defines the value as CANCELLED.
func decodeConfirmation(v string) eventv1.Confirmation {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "TENTATIVE":
		return eventv1.Confirmation_CONFIRMATION_TENTATIVE
	case "CONFIRMED":
		return eventv1.Confirmation_CONFIRMATION_CONFIRMED
	case "CANCELLED":
		return eventv1.Confirmation_CONFIRMATION_CANCELLED
	}
	return eventv1.Confirmation_CONFIRMATION_UNSPECIFIED
}

//nolint:misspell // RFC 5545 section 3.8.1.11 defines the value as CANCELLED.
func encodeConfirmation(c eventv1.Confirmation) string {
	switch c {
	case eventv1.Confirmation_CONFIRMATION_TENTATIVE:
		return "TENTATIVE"
	case eventv1.Confirmation_CONFIRMATION_CONFIRMED:
		return "CONFIRMED"
	case eventv1.Confirmation_CONFIRMATION_CANCELLED:
		return "CANCELLED"
	}
	return ""
}

// CLASS, section 3.8.1.3.
func decodeClassification(v string) eventv1.Classification {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "PUBLIC":
		return eventv1.Classification_CLASSIFICATION_PUBLIC
	case "PRIVATE":
		return eventv1.Classification_CLASSIFICATION_PRIVATE
	case "CONFIDENTIAL":
		return eventv1.Classification_CLASSIFICATION_CONFIDENTIAL
	}
	return eventv1.Classification_CLASSIFICATION_UNSPECIFIED
}

func encodeClassification(c eventv1.Classification) string {
	switch c {
	case eventv1.Classification_CLASSIFICATION_PUBLIC:
		return "PUBLIC"
	case eventv1.Classification_CLASSIFICATION_PRIVATE:
		return "PRIVATE"
	case eventv1.Classification_CLASSIFICATION_CONFIDENTIAL:
		return "CONFIDENTIAL"
	}
	return ""
}

// TRANSP, section 3.8.2.7.
func decodeTransparency(v string) eventv1.Transparency {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "OPAQUE":
		return eventv1.Transparency_TRANSPARENCY_OPAQUE
	case "TRANSPARENT":
		return eventv1.Transparency_TRANSPARENCY_TRANSPARENT
	}
	return eventv1.Transparency_TRANSPARENCY_UNSPECIFIED
}

func encodeTransparency(t eventv1.Transparency) string {
	switch t {
	case eventv1.Transparency_TRANSPARENCY_OPAQUE:
		return "OPAQUE"
	case eventv1.Transparency_TRANSPARENCY_TRANSPARENT:
		return "TRANSPARENT"
	}
	return ""
}
