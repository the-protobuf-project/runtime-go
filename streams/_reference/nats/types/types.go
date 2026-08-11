package types

// buildRawMessage creates a NatsRawMessage from a JetStreamPublishRequest
// and the already-encoded payload.
func BuildRawMessage(req JetStreamPublishRequest, data []byte) *NatsRawMessage {
	return &NatsRawMessage{
		Subject: req.Subject,
		Data:    data,
		Header:  req.Header,
		Sub:     req.Sub,
	}
}
