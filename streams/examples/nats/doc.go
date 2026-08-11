// Command nats demonstrates the streams contract over NATS, in both forms,
// against a live server.
//
// It is the same program twice: core NATS, where a message goes to whoever is
// listening and is then gone, and JetStream, where it is appended to a log a
// named consumer can resume from. The only lines that differ are the Connect
// call and what the contract will then let you ask for — which is the point of
// [streams.AsDurable].
//
// Run a server with JetStream enabled first:
//
//	docker compose -f ../../nats/docker/compose.yaml up -d
package main
