// Command timescale stores Go structs as time-series data.
//
// The struct is still the whole schema. What TimescaleDB adds is the part that
// is not storage — partitioning by time, reading a window, and reducing a window
// into buckets — and those take the descriptor the struct derived, which is what
// [database.Coll.Resource] is for.
//
//	docker compose -f ../../../docker/compose.yaml up -d timescaledb
//	go run ./simple/timescale
package main
