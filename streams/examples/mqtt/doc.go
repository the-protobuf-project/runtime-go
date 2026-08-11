// Command mqtt demonstrates the streams contract over MQTT 5.
//
// It shows what makes MQTT the odd one out: a named consumer's session really
// does hold what it has not handled, so [streams.Durable] is honest — but a
// session is a queue rather than a log, so [streams.AsPositioned] refuses.
//
// Run a broker first:
//
//	docker compose -f ../../docker/compose.yaml up -d mqtt
package main
