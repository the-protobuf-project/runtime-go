// Command keyvalue stores records in Redis.
//
// Same contract, same descriptors, no query language. Reach for it where the
// access pattern is by key and the operational story is one fewer moving part
// than a relational database — and know what it gives up, which this example is
// mostly about.
//
//	docker compose -f ../../cache/docker/compose.yaml up -d redis
//	go run ./keyvalue
package main
