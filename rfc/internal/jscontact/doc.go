// Package jscontact converts between the legacy vCard model
// (protobuf.rfc6350.vcard.v1) and the canonical JSContact model
// (protobuf.rfc9553.card.v1), following RFC 9555 as updated by RFC 9982.
//
// **This package is what makes rule 14 executable.** The ontology says the
// newest published RFC in a lineage is what gets stored and the older one is
// an accepted edge format; without a conversion that claim lives only in
// comments. Everything else in docs/ontology.md is documentation. This is the
// part that runs.
//
// It is also the one place in this repository where two schema packages meet.
// AIP-215 forbids a proto message in one package referencing another, which
// is why `Vcard` and `Card` know nothing of each other -- but nothing forbids
// a *Go* package importing both generated trees, and that is exactly the seam
// the rule intends the conversion to live in.
//
// # Where this departs from RFC 9555, and why
//
// Section 2.1.1 requires an implementation converting a vCard without a UID
// to generate one for the Card's mandatory `uid`. **That is not done here.**
// RFC 9982 redefines `uid` as optional precisely to stop the ephemeral
// identifiers that requirement produced -- a generated uid differs on every
// conversion, so a re-import creates a duplicate rather than matching the
// existing record. 9982 updates 9555, and the later reading wins.
//
// # Identifier choice
//
// Section 2.1.2 leaves the map keys free, requiring only that they are valid
// Id values (RFC 9553 section 1.4.1). They are generated positionally here --
// "e1", "e2" for emails, "p1" for phones -- because a deterministic scheme
// makes the conversion reproducible, which the RFC recommends for uid and
// which is worth having everywhere.
package jscontact
