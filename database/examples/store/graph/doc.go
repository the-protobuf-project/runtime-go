// Command graph stores records and the connections between them.
//
// ArangoDB holds both: the documents are addressed by key like any other
// document store, and the edges between them are walked on the server. That
// second half is the reason to choose it over MongoDB, and it is what this
// example is about.
//
//	docker compose -f ../docker/compose.yaml up -d arangodb
//	go run ./graph
package main
