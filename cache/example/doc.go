// Command example runs the cache's three steps — build a client, declare the
// cache, select a database — against every backend this module supports, and
// exercises all four strategies over each.
//
// It is the readable form of what the tests assert. Redis and Dragonfly show
// every capability working; memcached shows what a backend missing four of them
// refuses, and that the same call sites survive it.
//
//	docker compose -f ../docker/compose.yaml up -d
//	go run .
//
// Each backend is attempted independently, so the program is worth running with
// only one of the three up.
package main
