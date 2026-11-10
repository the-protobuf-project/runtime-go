// Package batchconn gathers the packets an MQTT client writes into as few
// writes as the network takes, for the MQTT and OPC UA PubSub providers.
//
// paho writes every packet on its own, a header and a body; over TLS each is a
// record and a system call, so a busy publisher and every PUBACK a consumer
// returns pay twice per packet. [Dialer] gives autopaho connections that queue
// those writes and send whatever has gathered in one go, as runtime-rs's MQTT
// client does.
package batchconn
