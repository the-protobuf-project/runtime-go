// Command redis demonstrates the streams contract in all three of the Redis
// provider's delivery modes, against a live Redis.
//
// Immediate over pub/sub, scheduled on a TTL expiry, and durable over Redis
// Streams — the last of which shows a consumer taking a message, dying without
// acknowledging it, and a second consumer being handed it back.
//
// The only provider-specific lines are the client you build and the Connect
// call; everything after that is the interface, so pointing this at another
// backend means changing those and nothing else.
//
// Run a server with keyspace notifications enabled first, which the scheduled
// mode needs:
//
//	docker compose -f ../../docker/compose.yaml up -d
package main
