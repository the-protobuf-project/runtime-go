// Command kafka demonstrates the streams contract over Apache Kafka.
//
// It shows the one thing only this provider can do: [streams.PartitionKey]
// decides which messages are ordered relative to each other, because Kafka
// orders within a partition and nowhere else.
//
// Run a broker first:
//
//	docker compose -f ../../docker/compose.yaml up -d kafka
package main
