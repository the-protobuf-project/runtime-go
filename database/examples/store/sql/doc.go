// Command sql stores records in a relational database.
//
// It runs against SQLite in memory, so it needs nothing installed — the same
// code runs against PostgreSQL or MySQL by changing the line that opens the
// client, which is the point.
//
//	go run ./sql
package main
