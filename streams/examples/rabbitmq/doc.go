// Command rabbitmq demonstrates the streams contract over RabbitMQ.
//
// It shows the one thing this provider does better than any other here: a true
// negative acknowledgement, so [streams.Delivery.Nak] hands a message straight
// back rather than waiting out a timeout.
//
// Run a broker first:
//
//	docker compose -f ../../docker/compose.yaml up -d rabbitmq
package main
