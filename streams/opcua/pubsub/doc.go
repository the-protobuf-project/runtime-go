// Package pubsub delivers [streams] over OPC UA PubSub (Part 14) on an MQTT
// broker, in the JSON message mapping.
//
// Where client/server (the opcua package) reads and writes a server's nodes,
// PubSub has no server: a publisher sends DataSets, the values of a set of
// named fields, to a broker, and any number of readers take them from there.
// The values are OPC UA's own typed DataValues, so a SCADA system, a
// historian, or Microsoft's OPC Publisher on the same broker reads what this
// provider writes, and this provider reads what they write. It writes the
// same bytes runtime-rs's PubSub provider writes, so the two interoperate.
//
// # The mapping
//
//   - The provider is a PubSub connection: one PublisherId on one broker.
//   - A stream is a WriterGroup: id `<group>` on this publisher, or
//     `<publisher>/<group>` to read another's.
//   - Subject `<writer>` is a DataSetWriter, all of its fields; subject
//     `<writer>/<field>` is one field of that writer's DataSet.
//   - Publish is one DataSetMessage in one NetworkMessage, at QoS 1,
//     returning after the PUBACK; [streams.Batch] puts several DataSetMessages
//     in one NetworkMessage.
//   - Subscribe is an MQTT subscription to the writer's topics, delivering one
//     message per field.
//
// Topics follow Part 14's MQTT mapping (7.3.5):
//
//	<prefix>/json/data/<PublisherId>/<WriterGroup>/<DataSetWriter>      data, not retained
//	<prefix>/json/metadata/<PublisherId>/<WriterGroup>/<DataSetWriter>  ua-metadata, retained
//
// with [DefaultPrefix], opcua, unless [WithPrefix] says otherwise. A
// subscription also listens on the writer-less `.../<WriterGroup>` topic,
// which Part 14 lets a publisher use for every writer of a group, and keeps
// only its own writer's DataSetMessages there.
//
// Messages are Part 14 JSON (7.2.5): a NetworkMessage with MessageType
// ua-data, MessageId, PublisherId and WriterGroupName, holding DataSetMessages
// with DataSetWriterId, DataSetWriterName, SequenceNumber, Timestamp,
// MetaDataVersion once metadata is published, and a Payload of DataValues
// keyed by field name, each in Part 6's reversible JSON so its built-in type
// survives. [NetworkMessage] and [MetaDataMessage] encode and decode them on
// their own. UADP, the binary mapping, is not implemented. A DataSetWriter's
// numeric id is [WriterID], a 16-bit FNV-1a hash of its name, so two
// processes declaring the same writer agree without coordination; two names
// in one stream that hash alike are refused at Create.
//
// # Publish and subscribe
//
// Publish to `<writer>/<field>` takes one OPC UA value — a *ua.DataValue, a
// *ua.Variant, or anything gopcua holds as a Variant (see [DataValueOf]) —
// and sends a DataSet of that one field; publish to `<writer>` takes a
// [DataSet] and sends all of its fields in one DataSetMessage. Any other
// value is refused with an error wrapping [streams.ErrUnsupported]: a field
// is a typed OPC UA value, not something a codec makes. The id returned is
// the NetworkMessage's MessageId: [streams.ID] or a new ULID, and unlike
// client/server it travels. A batch's entries are `<MessageId>.<n>`, n
// counting the DataSetMessages of one NetworkMessage; a batch longer than
// [WithMaxDatasetMessages] (default 100) goes out as several.
//
// Subscribe delivers every field of every DataSetMessage of the writer as its
// own [streams.Message], on subject `<writer>/<field>`, coded [CodecName],
// its data the field's DataValue in OPC UA binary. [streams.Message.Decode]
// reads it into a *ua.DataValue, a *ua.Variant, or the value's own Go type.
// The fields of one DataSetMessage share its id. Retained data messages,
// which were not published after the subscription, are skipped, as are
// keep-alives.
//
// # Headers and expiry
//
// Headers (the stream's and the publish's) travel as MQTT 5 user properties
// on the PUBLISH that carries the NetworkMessage, beside its application/json
// content type; the Part 14 JSON is exactly as it would be without them, so
// another Part 14 reader is unaffected and an MQTT 5 tool sees them. Every
// message a subscription takes from that NetworkMessage carries them, as it
// does the user properties another publisher set. The stream's own headers
// are stamped on its ua-metadata messages too.
//
// [streams.Expires] is the MQTT 5 Message Expiry Interval on the data
// NetworkMessage, rounded up to whole seconds as on the mqtt package.
// [streams.Retain] is refused: Part 14 retains ua-metadata, not ua-data, and
// subscribers skip retained data.
//
// # Sequence numbers
//
// Each DataSetWriter numbers its DataSetMessages, counting up from 1 in this
// provider. A subscriber tracks the last number per publisher and writer: a
// jump forward is a gap, logged as a warning, counted in
// [Provider.SequenceGaps], and added to the
// streams_opcua_pubsub_sequence_gaps_total counter of [WithMeter]. A repeat
// of the last number is a QoS 1 redelivery and is dropped; a jump back is a
// publisher that restarted, and counting starts again there.
//
// # Metadata
//
// [Provider.PublishMetadata] announces a writer's DataSetMetaData as a
// retained ua-metadata message, so a reader that arrives later still learns
// the fields' names and types; [DataSet.Metadata] builds one from a
// DataSet's values. From then on the writer's DataSetMessages carry its
// ConfigurationVersion as MetaDataVersion. [Provider.Metadata] reads one
// back.
//
// # Refused, and allowed
//
//   - [streams.Durable] is refused. Part 14 is fire-and-forget at the
//     application layer: a DataSetReader keeps no position and acknowledges
//     nothing, and one MQTT acknowledgement would settle every field of a
//     NetworkMessage at once. The mqtt package's durable consumer is the
//     honest tool for queued delivery.
//   - [streams.Positioned] is refused: there is no log.
//   - [streams.TTL] is refused: MQTT cannot schedule.
//   - Publishing on a stream of another PublisherId is refused: it would be
//     sending as someone else.
//   - [streams.Group] is allowed and is an MQTT shared subscription,
//     `$share/<group>/...`: the broker hands each NetworkMessage to one
//     member. Each member sees a share of the sequence, so gap tracking is off
//     for a group.
//
// [streams.PartitionKey] is ignored.
//
// # The broker
//
// [Connect] dials as the mqtt package does (host:port or a URL, [WithTLS],
// [WithCredentials]) and owns its connections: [WithPublishConnections]
// (default 4) for publishing, each topic always on the same one so a writer's
// messages stay in order, and one clean connection per subscription, which
// ends the subscription's channel if it is lost rather than redialing, since
// what was published meanwhile is gone. Declarations are local, as on MQTT.
package pubsub
