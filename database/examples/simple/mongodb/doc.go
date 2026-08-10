// Command mongodb stores Go structs in MongoDB.
//
// There is no protobuf anywhere in this file. The struct is the schema: its
// tags name the columns, mark the key and the unique fields, and the layer
// underneath derives from them the same descriptor a generated one would carry.
//
//	docker compose -f ../../../docker/compose.yaml up -d mongodb
//	go run ./simple/mongodb
package main
