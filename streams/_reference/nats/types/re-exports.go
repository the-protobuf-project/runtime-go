package types

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream" // Key import for JetStream specifics
)

// --- Original Re-exported Types from user's file (Generally Correct) ---
type Jetstream = jetstream.JetStream                         // Main JetStream context
type Stream = jetstream.Stream                               // Interface for JetStream Streams
type StreamInfo = jetstream.StreamInfo                       // Struct for Stream information
type StreamConfig = jetstream.StreamConfig                   // Struct for Stream configuration
type DiscardPolicy = jetstream.DiscardPolicy                 // Values: DiscardOld, DiscardNew
type StorageType = jetstream.StorageType                     // Values: MemoryStorage, FileStorage
type RePublish = jetstream.RePublish                         // Struct for RePublish config
type RetentionPolicy = jetstream.RetentionPolicy             // Values: LimitsPolicy, InterestPolicy, WorkQueuePolicy
type PubAck = jetstream.PubAck                               // Struct for Publish Acknowledgment
type Consumer = jetstream.Consumer                           // Interface for JetStream Consumers
type ConsumerInfo = jetstream.ConsumerInfo                   // Struct for Consumer information
type ConsumerConfig = jetstream.ConsumerConfig               // Struct for Consumer configuration
type NatsRawMessage = nats.Msg                               // Struct for raw NATS messages1
type OrderedConsumerConfig = jetstream.OrderedConsumerConfig // Struct for Ordered Consumer configuration
type ConsumerPauseResponse = jetstream.ConsumerPauseResponse // Struct for Consumer pause/resume response
type PullMessagesOpt = jetstream.PullMessagesOpt             // Type: func(*PullOpt)
type JetstreamMessage = jetstream.Msg                        // Struct for JetStream messages

// Key Value types
type KeyValueConfig = jetstream.KeyValueConfig // Struct for KeyValue configuration
type KeyValueEntry = jetstream.KeyValueEntry   // Struct for KeyValue entries
type KeyValue = jetstream.KeyValue             // Interface
type KeyWatcher = jetstream.KeyWatcher         // Interface
type WatchOpt = jetstream.WatchOpt             // Type: func(*watchOpts)
type JetStreamError = jetstream.JetStreamError // Interface

// Nats Types (from core nats.go, often used with JetStream)
type Header = nats.Header             // Struct for NATS message headers
type Subscription = nats.Subscription // Struct for NATS subscriptions

// --- Original Struct Definitions from user's file ---
type JetStreamPublishRequest struct {
	Subject string        // Subject to publish to
	Data    []byte        // Data to publish, can be any type
	Header  Header        // Optional headers for the message
	Sub     *Subscription // Optional subscription to use for the publish
	Async   bool          // Whether to publish asynchronously
}

// JetStreamPublishState represents the state of asynchronous JetStream publishing.
type JetStreamPublishState struct {
	Pending  int
	Complete bool
}

// --- JetStream Enums & Policies (Constants for direct use) ---

// RetentionPolicy values (constants for the type aliased above)
const (
	LimitsPolicy    RetentionPolicy = jetstream.LimitsPolicy
	InterestPolicy  RetentionPolicy = jetstream.InterestPolicy
	WorkQueuePolicy RetentionPolicy = jetstream.WorkQueuePolicy
)

// DiscardPolicy values (constants for the type aliased above)
const (
	DiscardOld DiscardPolicy = jetstream.DiscardOld
	DiscardNew DiscardPolicy = jetstream.DiscardNew
)

// StorageType values (constants for the type aliased above)
const (
	FileStorage   StorageType = jetstream.FileStorage
	MemoryStorage StorageType = jetstream.MemoryStorage
)

// AckPolicy and its values
type AckPolicy = jetstream.AckPolicy // Alias the type
const (
	AckNone     AckPolicy = jetstream.AckNonePolicy     // No acks are required
	AckAll      AckPolicy = jetstream.AckAllPolicy      // Acking a message acks all previous ones
	AckExplicit AckPolicy = jetstream.AckExplicitPolicy // Each message requires an explicit ack (default)
)

// DeliverPolicy and its values
type DeliverPolicy = jetstream.DeliverPolicy // Alias the type
const (
	DeliverAll             DeliverPolicy = jetstream.DeliverAllPolicy             // Start with the first available message
	DeliverLast            DeliverPolicy = jetstream.DeliverLastPolicy            // Start with the last available message
	DeliverNew             DeliverPolicy = jetstream.DeliverNewPolicy             // Start with new messages sent after consumer creation
	DeliverByStartSequence DeliverPolicy = jetstream.DeliverByStartSequencePolicy // Start at a specific sequence
	DeliverByStartTime     DeliverPolicy = jetstream.DeliverByStartTimePolicy     // Start at messages from a specific time
	DeliverLastPerSubject  DeliverPolicy = jetstream.DeliverLastPerSubjectPolicy  // Start with the last message for each subject
)

// For Pull-based Consumer message fetching (PullMessagesOpt type already aliased as func(*PullOpt))
func PullMaxMessages(maxMessages int) PullMessagesOpt {
	return jetstream.PullMaxMessages(maxMessages)
}

// PullExpiry re-exports jetstream.PullExpiry under our session package.
func PullExpiry(d time.Duration) PullMessagesOpt {
	return jetstream.PullExpiry(d)
}

// FetchMaxWait re-exports jetstream.FetchMaxWait under our session package.
func FetchMaxWait(timeout time.Duration) jetstream.FetchOpt {
	return jetstream.FetchMaxWait(timeout)
}
