package rfc

// contact.go is the vCard side of the contact lineage: a Contact, and the four
// things it can become.
//
// Three of those four are encodings of the same model — text/vcard,
// application/vcard+xml, application/vcard+json — and the fourth is a different
// model entirely. `Card` crosses from RFC 6350 to RFC 9553, which is the
// conversion RFC 9555 specifies and the one that makes this package more than a
// serializer.

import (
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc9553/card/v1"

	"github.com/the-protobuf-project/runtime-go/rfc/internal/jscontact"
	"github.com/the-protobuf-project/runtime-go/rfc/internal/vcard"
)

// ContactSource converts a vCard Contact into the forms RFC 6350 and its
// successors define.
//
// Obtained from [Contact]; the methods are the forms.
type ContactSource struct {
	contact  *vcardv1.Contact
	validate bool
}

// Contact begins a conversion from a vCard Contact.
//
//	text, err := rfc.Contact(c).VCard()
//	card, err := rfc.Contact(c).Card()
//
// Go forbids methods on a type from another package, so this is not decoration
// — it is the only way to get receiver syntax over a message that belongs to
// protoc-gen-go. It costs one call and no allocation.
func Contact(c *vcardv1.Contact) *ContactSource {
	return &ContactSource{contact: c}
}

// Validate requires the contact to satisfy its buf.validate rules before any
// method on this source produces a result.
//
//	text, err := rfc.Contact(c).Validate().VCard()
//
// Chainable rather than terminal; see validate.go for why. Use [ContactSource.Err]
// to check validity on its own.
func (s *ContactSource) Validate() *ContactSource {
	s.validate = true
	return s
}

// Err reports whether the contact satisfies its rules, for a caller who wants
// the check without a conversion.
//
//	if err := rfc.Contact(c).Validate().Err(); err != nil {
//
// Returns nil when Validate was not called: nothing was asked for.
func (s *ContactSource) Err() error {
	return s.checked()
}

func (s *ContactSource) checked() error {
	if !s.validate {
		return nil
	}
	return check("contact", s.contact)
}

// VCard renders the contact as text/vcard, RFC 6350 <https://www.rfc-editor.org/rfc/rfc6350.html>.
//
// Fails when the contact has no FN: section 6.2.1 makes it the one mandatory
// property, so a Contact without one has no valid vCard rendering.
func (s *ContactSource) VCard() (string, error) {
	if err := s.checked(); err != nil {
		return "", err
	}
	out, err := vcard.Encode(s.contact)
	return out, fail("contact", "vcard", err)
}

// XCard renders the contact as application/vcard+xml, RFC 6351 <https://www.rfc-editor.org/rfc/rfc6351.html>.
//
// Note one loss the format imposes: RFC 6351 maps a compound value to named
// child elements, and RFC 9554's added ADR and N components have no registered
// element names, so they do not survive the crossing. See the package doc.
func (s *ContactSource) XCard() ([]byte, error) {
	if err := s.checked(); err != nil {
		return nil, err
	}
	out, err := vcard.EncodeXCard(s.contact)
	return out, fail("contact", "xcard", err)
}

// JCard renders the contact as application/vcard+json, RFC 7095 <https://www.rfc-editor.org/rfc/rfc7095.html>.
func (s *ContactSource) JCard() ([]byte, error) {
	if err := s.checked(); err != nil {
		return nil, err
	}
	out, err := vcard.EncodeJCard(s.contact)
	return out, fail("contact", "jcard", err)
}

// Card converts the contact to a JSContact Card, RFC 9555 <https://www.rfc-editor.org/rfc/rfc9555.html> as updated by
// RFC 9982 <https://www.rfc-editor.org/rfc/rfc9982.html>.
//
// This is the conversion that makes JSContact the canonical model and vCard an
// edge format: what a service stores is the Card, whichever of the two a client
// sent. It is a model change rather than a re-encoding, so it is not lossless in
// the way the three encodings above are — RFC 9555 section 1.1 is candid that
// the two disagree about what is one property and what is several.
func (s *ContactSource) Card() (*cardv1.Card, error) {
	if err := s.checked(); err != nil {
		return nil, err
	}
	out, err := jscontact.FromVcard(s.contact)
	return out, fail("contact", "card", err)
}
