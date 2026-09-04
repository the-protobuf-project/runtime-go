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

// TestValidateRunsFieldRules checks the chain reaches protovalidate at all,
// using a rule that is trivial to violate: RFC 6350 section 6.2.1 makes FN
// mandatory, and the schema says so with repeated.min_items.
func TestValidateRunsFieldRules(t *testing.T) {
	contact, err := rfc.VCard(sampleVCard).Contact()
	if err != nil {
		t.Fatal(err)
	}

	// Valid as parsed.
	if validErr := rfc.Contact(contact).Validate().Err(); validErr != nil {
		t.Errorf("a parsed contact should satisfy its own rules: %v", validErr)
	}

	// And invalid once the mandatory property is removed.
	contact.DisplayNames = nil
	err = rfc.Contact(contact).Validate().Err()
	if err == nil {
		t.Fatal("expected a violation for a contact with no FN")
	}
	var invalid *rfc.ValidationError
	if !errors.As(err, &invalid) {
		t.Fatalf("error is not a *rfc.ValidationError: %T", err)
	}
	if invalid.Subject != "contact" {
		t.Errorf("subject = %q, want contact", invalid.Subject)
	}
}

// TestValidateGatesTheChain pins that Validate stops a conversion rather than
// merely reporting alongside it: an invalid message must not produce output.
func TestValidateGatesTheChain(t *testing.T) {
	contact, err := rfc.VCard(sampleVCard).Contact()
	if err != nil {
		t.Fatal(err)
	}
	contact.DisplayNames = nil

	if _, err := rfc.Contact(contact).Validate().Card(); err == nil {
		t.Error("Validate did not stop the conversion")
	}
	// Without Validate the same call is the caller's business, not this
	// package's -- the chain does nothing it was not asked to do.
	if _, err := rfc.Contact(contact).Card(); err != nil {
		t.Errorf("an unvalidated conversion should not check: %v", err)
	}
}

// TestValidateRunsMessageCEL is the one that matters.
//
// The schemas carry message-scoped CEL rules that no gate in the protobuf
// repository ever executed -- buf build, buf lint and api-linter all pass on a
// broken expression, because none of them evaluates CEL. This test is the first
// thing that runs them, which is why it asserts the rule fires *and* that a
// conforming message passes: an expression that is simply wrong would otherwise
// look like a working rule.
//
// The rule under test is alarm.display_needs_description, RFC 5545 section
// 3.6.6: a DISPLAY alarm must carry a description.
func TestValidateRunsMessageCEL(t *testing.T) {
	const withAlarm = "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//Example//Test//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1@example.com\r\n" +
		"DTSTAMP:20260101T090000Z\r\n" +
		"DTSTART:20260301T090000Z\r\n" +
		"SUMMARY:Standup\r\n" +
		"BEGIN:VALARM\r\n" +
		"ACTION:DISPLAY\r\n" +
		"TRIGGER:-PT10M\r\n" +
		"DESCRIPTION:Standup in 10 minutes\r\n" +
		"END:VALARM\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	event, err := rfc.ICalendar(withAlarm).Validate().Event()
	if err != nil {
		t.Fatalf("a conforming DISPLAY alarm must pass its CEL rule: %v", err)
	}
	if len(event.GetAlarms()) != 1 {
		t.Fatalf("got %d alarms, want 1", len(event.GetAlarms()))
	}

	// Now violate it: a DISPLAY alarm with no description.
	event.GetAlarms()[0].Description = ""
	err = rfc.Event(event).Validate().Err()
	if err == nil {
		t.Fatal("alarm.display_needs_description did not fire; the CEL rule is not being evaluated")
	}
	if !strings.Contains(err.Error(), "DISPLAY alarm requires description") {
		t.Errorf("violation did not name the rule: %v", err)
	}
}

// TestValidateIsOptional pins that the chain is unchanged when Validate is not
// called, so adding it to the API costs an existing caller nothing.
func TestValidateIsOptional(t *testing.T) {
	withoutValidate, err := rfc.VCard(sampleVCard).Contact()
	if err != nil {
		t.Fatal(err)
	}
	withValidate, err := rfc.VCard(sampleVCard).Validate().Contact()
	if err != nil {
		t.Fatalf("a valid document should pass: %v", err)
	}
	if withoutValidate.GetDisplayNames()[0] != withValidate.GetDisplayNames()[0] {
		t.Error("Validate changed the parsed result")
	}
	// Err on a source that was never asked to validate reports nothing.
	if err := rfc.Contact(withoutValidate).Err(); err != nil {
		t.Errorf("Err without Validate = %v, want nil", err)
	}
}
