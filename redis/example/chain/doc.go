// Command chain demonstrates the whole runtime-go Redis provider against a live
// server: the client, a named database, and every handler that hangs off it.
//
//	docker compose -f ../../docker/compose.yaml up -d
//	go run ./example/chain
package main
