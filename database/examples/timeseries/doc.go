// Command timeseries partitions a resource by time and reduces over it.
//
// TimescaleDB is PostgreSQL with an extension, so everything the relational
// example does works here unchanged — the driver embeds that one. What this
// example is about is the part that is not SQL: a hypertable, a window, and a
// reduction that runs in the database rather than in this process.
//
//	docker compose -f ../docker/compose.yaml up -d timescaledb
//	go run ./timeseries
package main
