// Package rfc converts between the wire formats the IETF specified for contacts
// and calendars and the protobuf schemas that model them.
//
// vCard, xCard, jCard, iCalendar, jCal and VAVAILABILITY go in; the messages
// published at buf.build/the-protobuf-project/rfc come out, and back again.
//
// # Why a conversion, and not a parser
//
// Two of these formats are not encodings of the same thing. RFC 6350's vCard and
// RFC 9553's JSContact are different models of a contact, and the newer one is
// the one a service should store: a name in JSContact is an ordered list of
// typed components, where vCard's N is five positional slots that a name outside
// the Western convention does not fit. Storing the older model means losing
// structure the newer one would have kept.
//
// So this package does two jobs that look alike and are not. Re-encoding moves a
// model between its own serializations and loses nothing — vCard, xCard and jCard
// are one model in three syntaxes. Converting moves between models, and RFC 9555
// specifies how, element by element. The API keeps them in the same shape because
// a caller rarely cares which one they are asking for, but the loss
// characteristics differ and the method docs say where.
//
// # The shape of the API
//
// Every conversion is a two-link chain. The factory names what you have; the
// method names what you want:
//
//	card, err    := rfc.VCard(text).Card()        // vCard document  -> JSContact
//	text, err    := rfc.Card(card).VCard()        // JSContact       -> vCard document
//	contact, err := rfc.JCard(data).Contact()     // jCard document  -> vCard model
//	data, err    := rfc.Contact(c).XCard()        // vCard model     -> xCard document
//	event, err   := rfc.ICalendar(text).Event()   // iCalendar       -> Event
//	data, err    := rfc.Event(e).JCal()           // Event           -> jCal
//
// There is no To, no From and no Bytes in a method name: a verb prefix makes the
// call harder to recall without saying anything the signature does not, and the
// same vocabulary should read the same way in every language runtime rather than
// only this one.
//
// The factory is not decoration. Go forbids methods on a type from another
// package, and these messages belong to protoc-gen-go, so a receiver has to be
// borrowed. It costs one call and no allocation.
//
// The first and second lines above are the whole of the storage pipeline: a
// client may send either representation, the service stores the newer one, and
// either can be served back. That a vCard saved through this package reads back
// as a vCard rendered from a Card is the design, not an accident — see
// [CardSource.Contact] for what that costs.
//
// # Validation
//
// Any chain can be asked to check the message against the buf.validate rules
// the schemas carry, by inserting one link:
//
//	card, err := rfc.VCard(text).Validate().Card()
//	err       := rfc.Contact(c).Validate().Err()
//
// It is a link rather than a separate call because a caller holding a document
// has no message to validate until it is parsed — so the check has to run
// inside the chain, at the point where one exists. `Err` is the terminal for a
// caller who wants the verdict and no conversion.
//
// Without `Validate` nothing is checked. That is deliberate: a conversion is not
// the right place to impose a policy the caller did not ask for, and a document
// that fails its rules may still be worth reading.
//
// The rules are message-scoped — ranges, patterns, required fields, enum
// membership, and the CEL relations a field constraint cannot express, such as
// RFC 5545 section 3.6.6's "a DISPLAY alarm requires a description". Anything
// needing state a message does not carry is the server's job.
//
// # What does not round-trip, and why
//
// Every loss below is the format's, not this package's, and each is asserted by a
// test rather than assumed.
//
//   - xCard cannot carry RFC 9554's added ADR and N components. RFC 6351 maps a
//     compound value to named child elements and 9554 registers no names for the
//     eleven address components it adds, so they are dropped crossing into XML.
//     jCard has no such problem: RFC 7095 writes a compound value as a positional
//     array, which simply gets longer.
//
//   - jCard and xCard lowercase property names, so an extension key's original
//     case does not survive. That is RFC 7095 section 3.3 and RFC 6351 section
//     3.2 respectively.
//
//   - A vCard birthday of "circa 1800" — which RFC 6350 section 6.2.5 permits —
//     has no JSContact form. It converts to nothing rather than to a wrong date.
//
//   - TITLE and ROLE are two vCard properties and one JSContact object; IMPP and
//     SOCIALPROFILE are two and one. Converting back splits them again, so the
//     data survives but the property it lands in may differ.
//
// One departure is this package's own rather than a format's. RFC 9555 section
// 2.1.1 requires a converter to generate a uid when the vCard has none; RFC 9982
// then made uid optional precisely because a generated one differs on every run,
// so re-importing the same vCard creates a duplicate instead of matching. The
// later RFC wins and no uid is invented.
//
// # Errors
//
// A failure names the conversion it happened in — "rfc: jcal to event: …" — and
// wraps the codec's own error, so [errors.Is] and [errors.As] still reach it. A
// chain that runs two steps reports the step that broke rather than the
// expression that contained it.
package rfc
