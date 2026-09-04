// Package contentline implements the content-line syntax shared by vCard
// (RFC 6350 sections 3.2-3.4) and iCalendar (RFC 5545 sections 3.1-3.2).
//
// The two RFCs define the same wire grammar -- folding at 75 octets, the
// [group "."] name *(";" param) ":" value production, and the same TEXT
// escape set. Sharing it means a folding or escaping fix lands in both
// codecs at once, which is the whole reason this package exists rather than
// each codec carrying its own copy.
package contentline
