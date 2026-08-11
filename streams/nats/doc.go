// Package nats delivers [streams] over NATS, in two forms.
//
// [Connect] is core NATS: a message goes to whoever is listening at that
// moment and is then gone. It satisfies [streams.Publisher] and
// [streams.Subscriber] and nothing beyond them, because that is all core NATS
// does — there is no log behind it to replay and no record of what anyone
// handled.
//
// [ConnectJetStream] is JetStream: the same subjects, backed by a stored log.
// It satisfies [streams.Durable] and [streams.Positioned] as well, so a
// consumer can be named, resume where that name left off, and have a message
// redelivered until it says the work is done.
//
// The two are separate constructors rather than one with a flag because the
// difference is not a setting. A program that believes it has at-least-once
// delivery and actually has at-most-once loses messages under exactly the
// conditions it added the durability for, and [streams.AsDurable] is what tells
// the two apart — it succeeds on the JetStream manager and refuses, by name, on
// the core one.
package nats
