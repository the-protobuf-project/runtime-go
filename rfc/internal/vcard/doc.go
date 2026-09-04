// Package vcard converts between RFC 6350's three encodings and the
// protobuf.rfc6350.vcard.v1 schema.
//
// One model, three syntaxes: text/vcard (RFC 6350), application/vcard+xml
// (RFC 6351) and application/vcard+json (RFC 7095). The content-line grammar
// the text form is built on lives in internal/contentline, because iCalendar
// shares it.
//
// The three are kept in one package rather than split per encoding because they
// are not independent: decodeLines and contentLines are written once and reused,
// so only the syntax layer differs. That sharing is what the cross-format tests
// exercise — two encodings of one model cannot legitimately disagree, and when
// they did it was a URI-escaping bug in the jCard path that a single-format
// round trip could not have seen.
package vcard
