package rfc

// card.go is the JSContact side of the contact lineage.
//
// A Card converts back to a Contact, and — because a caller who has a Card and
// wants bytes should not have to name the intermediate — straight through to
// any of the three vCard encodings. Those pass-throughs are the reason this file
// exists rather than the methods living on ContactSource: the conversion runs in
// the direction the caller asked for, not the direction the code is organized
// in.

import (
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc6350/vcard/v1"
	"buf.build/gen/go/the-protobuf-project/rfc/protocolbuffers/go/protobuf/rfc9553/card/v1"

	"github.com/the-protobuf-project/runtime-go/rfc/internal/jscontact"
)

// CardSource converts a JSContact Card into the vCard model and its encodings.
//
// Obtained from [Card].
type CardSource struct {
	card     *cardv1.Card
	validate bool
}

// Card begins a conversion from a JSContact Card.
//
//	contact, err := rfc.Card(k).Contact()
//	text, err    := rfc.Card(k).VCard()
//
// The second form is the whole of rule 14's read path in one expression: what
// the service stored, rendered as what the client asked for.
func Card(c *cardv1.Card) *CardSource {
	return &CardSource{card: c}
}

// Contact converts the Card to a vCard Contact, RFC 9555 <https://www.rfc-editor.org/rfc/rfc9555.html> section 3.
//
// Not the strict inverse of [ContactSource.Card], and section 1.1 says why: the
// two models disagree about what is one property and what is several. TITLE and
// ROLE are two vCard properties and one JSContact object; PHOTO, SOUND and LOGO
// are three and one. The data survives, the encoding may not.
func (s *CardSource) Contact() (*vcardv1.Contact, error) {
	if err := s.checked(); err != nil {
		return nil, err
	}
	out, err := jscontact.ToVcard(s.card)
	return out, fail("card", "contact", err)
}

// VCard converts the Card and renders it as text/vcard.
//
// Equivalent to Contact followed by [ContactSource.VCard]; the error names
// whichever step failed.
func (s *CardSource) VCard() (string, error) {
	if err := s.checked(); err != nil {
		return "", err
	}
	c, err := s.Contact()
	if err != nil {
		return "", err
	}
	return Contact(c).VCard()
}

// Validate requires the card to satisfy its buf.validate rules before any
// method on this source produces a result.
//
// Chainable rather than terminal; see validate.go for why.
func (s *CardSource) Validate() *CardSource {
	s.validate = true
	return s
}

// Err reports whether the card satisfies its rules, without converting.
// Returns nil when Validate was not called.
func (s *CardSource) Err() error { return s.checked() }

func (s *CardSource) checked() error {
	if !s.validate {
		return nil
	}
	return check("card", s.card)
}

// XCard converts the Card and renders it as application/vcard+xml.
func (s *CardSource) XCard() ([]byte, error) {
	if err := s.checked(); err != nil {
		return nil, err
	}
	c, err := s.Contact()
	if err != nil {
		return nil, err
	}
	return Contact(c).XCard()
}

// JCard converts the Card and renders it as application/vcard+json.
func (s *CardSource) JCard() ([]byte, error) {
	if err := s.checked(); err != nil {
		return nil, err
	}
	c, err := s.Contact()
	if err != nil {
		return nil, err
	}
	return Contact(c).JCard()
}
