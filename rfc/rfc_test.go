package rfc_test

// The public API is a thin layer over codecs that have their own tests, so what
// is worth asserting here is what only this layer can get wrong: that a chain
// reaches the right codec, that a two-step chain reports the step that failed,
// and that the round trips the package doc advertises actually hold.

import (
	"errors"
	"strings"
	"testing"

	"github.com/the-protobuf-project/runtime-go/rfc"
)

const sampleVCard = "BEGIN:VCARD\r\n" +
	"VERSION:4.0\r\n" +
	"FN:Jane Rodriguez\r\n" +
	"N:Rodriguez;Jane;;Dr;PhD;Garcia;Jr\r\n" +
	"EMAIL;TYPE=work:jane@example.com\r\n" +
	"TEL;TYPE=\"work,voice\":tel:+1-555-0100\r\n" +
	"ADR;TYPE=home:;;1 Example Way;Springfield;IL;62704;USA\r\n" +
	"END:VCARD\r\n"

const sampleICalendar = "BEGIN:VCALENDAR\r\n" +
	"VERSION:2.0\r\n" +
	"PRODID:-//Example//Test//EN\r\n" +
	"BEGIN:VEVENT\r\n" +
	"UID:event-1@example.com\r\n" +
	"DTSTAMP:20260101T090000Z\r\n" +
	"DTSTART:20260301T090000Z\r\n" +
	"SUMMARY:Standup\r\n" +
	"END:VEVENT\r\n" +
	"END:VCALENDAR\r\n"

// TestContactChains walks every method reachable from a vCard document, which
// is the surface a caller actually touches.
func TestContactChains(t *testing.T) {
	contact, err := rfc.VCard(sampleVCard).Contact()
	if err != nil {
		t.Fatalf("vcard -> contact: %v", err)
	}
	if got := contact.GetDisplayNames(); len(got) != 1 || got[0] != "Jane Rodriguez" {
		t.Errorf("FN = %q", got)
	}

	for _, tc := range []struct {
		name string
		run  func() (int, error)
	}{
		{"VCard", func() (int, error) { s, err := rfc.Contact(contact).VCard(); return len(s), err }},
		{"XCard", func() (int, error) { b, err := rfc.Contact(contact).XCard(); return len(b), err }},
		{"JCard", func() (int, error) { b, err := rfc.Contact(contact).JCard(); return len(b), err }},
	} {
		n, err := tc.run()
		if err != nil {
			t.Errorf("contact -> %s: %v", tc.name, err)
			continue
		}
		if n == 0 {
			t.Errorf("contact -> %s produced nothing", tc.name)
		}
	}
}

// TestVCardToCardToVCard is the storage pipeline the package doc describes: a
// client sends vCard, the service stores JSContact, and either can be served
// back. It has to survive both crossings.
func TestVCardToCardToVCard(t *testing.T) {
	card, err := rfc.VCard(sampleVCard).Card()
	if err != nil {
		t.Fatalf("vcard -> card: %v", err)
	}
	if got := card.GetNameComponents().GetDisplayName(); got != "Jane Rodriguez" {
		t.Errorf("card name = %q", got)
	}
	// RFC 9554's second surname must have crossed as its own component rather
	// than being folded into the family name.
	var surname2 string
	for _, c := range card.GetNameComponents().GetComponents() {
		if c.GetKind().String() == "NAME_COMPONENT_KIND_SURNAME2" {
			surname2 = c.GetValue()
		}
	}
	if surname2 != "Garcia" {
		t.Errorf("surname2 = %q, want Garcia", surname2)
	}

	// And back out as a document, in one expression.
	text, err := rfc.Card(card).VCard()
	if err != nil {
		t.Fatalf("card -> vcard: %v", err)
	}
	if !strings.Contains(text, "FN:Jane Rodriguez") {
		t.Errorf("round trip lost FN:\n%s", text)
	}

	// RFC 9982: no uid is invented for a vCard that carried none.
	if got := card.GetJscontactUid(); got != "" {
		t.Errorf("jscontact_uid = %q; RFC 9982 requires it not be generated", got)
	}
}

// TestCrossEncoding asserts the three vCard syntaxes agree, which is the
// property that makes them one model rather than three.
func TestCrossEncoding(t *testing.T) {
	contact, err := rfc.VCard(sampleVCard).Contact()
	if err != nil {
		t.Fatal(err)
	}
	jcard, err := rfc.Contact(contact).JCard()
	if err != nil {
		t.Fatal(err)
	}
	viaJCard, err := rfc.JCard(jcard).Contact()
	if err != nil {
		t.Fatalf("jcard -> contact: %v", err)
	}
	if got := viaJCard.GetDisplayNames(); len(got) != 1 || got[0] != "Jane Rodriguez" {
		t.Errorf("FN through jCard = %q", got)
	}
	// jCard carries RFC 9554's components, where xCard cannot -- see the
	// package doc. Assert the half that should survive.
	if len(viaJCard.GetNameComponents().GetSecondarySurnames()) != 1 {
		t.Error("jCard dropped the secondary surname, which RFC 7095 can carry")
	}
}

// TestCalendarChains covers the calendar lineage, which has no canonical model
// to convert to and therefore only encodes and decodes.
func TestCalendarChains(t *testing.T) {
	event, err := rfc.ICalendar(sampleICalendar).Event()
	if err != nil {
		t.Fatalf("icalendar -> event: %v", err)
	}
	if got := event.GetSummary(); got != "Standup" {
		t.Errorf("summary = %q", got)
	}

	text, err := rfc.Event(event).ICalendar()
	if err != nil {
		t.Fatalf("event -> icalendar: %v", err)
	}
	if !strings.Contains(text, "SUMMARY:Standup") {
		t.Errorf("round trip lost SUMMARY:\n%s", text)
	}

	data, err := rfc.Event(event).JCal()
	if err != nil {
		t.Fatalf("event -> jcal: %v", err)
	}
	back, err := rfc.JCal(data).Event()
	if err != nil {
		t.Fatalf("jcal -> event: %v", err)
	}
	if got := back.GetSummary(); got != "Standup" {
		t.Errorf("summary through jCal = %q", got)
	}
}

// TestErrorNamesTheStep pins that a failure says which conversion broke, and
// that a two-step chain blames the step rather than the expression.
func TestErrorNamesTheStep(t *testing.T) {
	// A single step: the document is not a vCard at all.
	_, err := rfc.VCard("not a vcard").Contact()
	if err == nil {
		t.Fatal("expected an error from a malformed document")
	}
	var conv *rfc.Error
	if !errors.As(err, &conv) {
		t.Fatalf("error is not an *rfc.Error: %T", err)
	}
	if conv.From != "vcard" || conv.To != "contact" {
		t.Errorf("error names %s -> %s, want vcard -> contact", conv.From, conv.To)
	}
	if !strings.HasPrefix(err.Error(), "rfc: vcard to contact: ") {
		t.Errorf("message = %q", err.Error())
	}

	// Two steps: parsing fails first, so the error must still name the parse
	// and not the conversion that never ran.
	_, err = rfc.VCard("not a vcard").Card()
	if !errors.As(err, &conv) {
		t.Fatalf("chained error is not an *rfc.Error: %T", err)
	}
	if conv.To != "contact" {
		t.Errorf("chained error names the wrong step: %s -> %s", conv.From, conv.To)
	}

	// Unwrap must still reach the codec's own error.
	if errors.Unwrap(conv) == nil {
		t.Error("Unwrap lost the underlying cause")
	}
}

// TestEncodeRejectsIncomplete checks the errors that come from the model rather
// than the document: RFC 6350 makes FN mandatory, and the encoder must say so
// rather than emit an invalid vCard.
func TestEncodeRejectsIncomplete(t *testing.T) {
	contact, err := rfc.VCard(sampleVCard).Contact()
	if err != nil {
		t.Fatal(err)
	}
	contact.DisplayNames = nil

	if _, err := rfc.Contact(contact).VCard(); err == nil {
		t.Error("expected an error for a contact with no FN")
	}
}
