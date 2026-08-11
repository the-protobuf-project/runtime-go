// Package core is the code every streams provider would otherwise copy.
//
// It sits between [streams] and the backends: streams defines the contract and
// says what a provider must do, and this package holds the mechanical parts of
// doing it. A provider imports core; core never imports a provider.
//
// Nothing here decides behavior. Framing a message so its id survives the trip,
// generating that id, and deciding whether a subject is one the stream declared
// are the three things Redis and NATS had both written before this package
// existed — which is two chances to frame a message differently and have a
// payload published through one provider fail to decode through the other.
package core
