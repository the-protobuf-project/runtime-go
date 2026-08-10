// Command cached puts a read-through cache in front of a store.
//
// This is the composition the two modules were built for: the cache module
// collapses concurrent misses and remembers absences, and the database module
// holds the records. Neither knows about the other — cached.Wrap is the whole
// of the wiring.
//
//	docker compose -f ../../cache/docker/compose.yaml up -d redis
//	go run ./cached
package main
