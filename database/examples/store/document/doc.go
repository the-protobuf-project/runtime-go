// Command document stores records in MongoDB and watches them change.
//
// The columns become the document's fields, so what lands in MongoDB is
// queryable with the tools people already use rather than a blob only this
// program can read. What MongoDB adds over the key-value example is a server
// that filters, and a change stream — which is the reason to reach for it when
// something has to react to writes.
//
//	docker compose -f ../docker/compose.yaml up -d mongodb
//	go run ./document
package main
