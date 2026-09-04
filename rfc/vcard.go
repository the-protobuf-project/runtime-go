package rfc

// vcard.go is the reading half of the contact lineage: a document in one of
// RFC 6350's three encodings, and the two models it can become.
//
// Each of the three readers offers the same two methods, because the encoding a
// document arrived in should not change what a caller can ask for. `Contact`
// stops at the model the document is written in; `Card` carries on to the
// canonical one, which is what a service storing JSContact wants from any of
// them.

import (
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc9553/card/v1"

	"github.com/the-protobuf-project/runtime-go/rfc/internal/vcard"
)

// VCardSource converts a text/vcard document, RFC 6350 <https://www.rfc-editor.org/rfc/rfc6350.html>.
//
// Obtained from [VCard].
type VCardSource struct {
	text string
}

// VCard begins a conversion from a text/vcard document.
//
//	contact, err := rfc.VCard(text).Contact()
//	card, err    := rfc.VCard(text).Card()
//
// The second form is rule 14's write path in one expression: what the client
// sent, as what the service stores.
func VCard(text string) *VCardSource {
	return &VCardSource{text: text}
}

// Contact parses the document into a vCard Contact.
func (s *VCardSource) Contact() (*vcardv1.Contact, error) {
	out, err := vcard.Decode(s.text)
	return out, fail("vcard", "contact", err)
}

// Card parses the document and converts it to a JSContact Card.
func (s *VCardSource) Card() (*cardv1.Card, error) {
	c, err := s.Contact()
	if err != nil {
		return nil, err
	}
	return Contact(c).Card()
}

// XCardSource converts an application/vcard+xml document, RFC 6351 <https://www.rfc-editor.org/rfc/rfc6351.html>.
//
// Obtained from [XCard].
type XCardSource struct {
	data []byte
}

// XCard begins a conversion from an application/vcard+xml document.
func XCard(data []byte) *XCardSource {
	return &XCardSource{data: data}
}

// Contact parses the document into a vCard Contact.
func (s *XCardSource) Contact() (*vcardv1.Contact, error) {
	out, err := vcard.DecodeXCard(s.data)
	return out, fail("xcard", "contact", err)
}

// Card parses the document and converts it to a JSContact Card.
func (s *XCardSource) Card() (*cardv1.Card, error) {
	c, err := s.Contact()
	if err != nil {
		return nil, err
	}
	return Contact(c).Card()
}

// JCardSource converts an application/vcard+json document, RFC 7095 <https://www.rfc-editor.org/rfc/rfc7095.html>.
//
// Obtained from [JCard].
type JCardSource struct {
	data []byte
}

// JCard begins a conversion from an application/vcard+json document.
func JCard(data []byte) *JCardSource {
	return &JCardSource{data: data}
}

// Contact parses the document into a vCard Contact.
func (s *JCardSource) Contact() (*vcardv1.Contact, error) {
	out, err := vcard.DecodeJCard(s.data)
	return out, fail("jcard", "contact", err)
}

// Card parses the document and converts it to a JSContact Card.
func (s *JCardSource) Card() (*cardv1.Card, error) {
	c, err := s.Contact()
	if err != nil {
		return nil, err
	}
	return Contact(c).Card()
}
