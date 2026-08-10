// Command sql stores the same struct in a relational database.
//
// It is deliberately near-identical to the mongodb example: the only lines that
// differ are the ones that open a client. That is the claim this module makes,
// and this pair is where it is checked rather than asserted.
//
// It runs against SQLite in memory, so it needs nothing installed.
//
//	go run ./simple/sql
package main
