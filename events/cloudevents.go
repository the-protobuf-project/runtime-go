package events

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
)

// ContentType is the Content-Type of every delivery: a CloudEvents 1.0
// structured-mode JSON document.
const ContentType = "application/cloudevents+json; charset=utf-8"

// EventID identifies one version of one record: a record's etag changes with
// every write, so (name, etag) names one version.
func EventID(name, etag string) string { return digest(name, etag) }

// DeliveryName is where an event is recorded for a webhook. The id is
// derived, so recording the same change twice collides on the name.
func DeliveryName(webhook, eventID string) string {
	return webhook + "/deliveries/d" + digest(webhook, eventID)[:31]
}

// CloudEvent renders a CloudEvents 1.0 structured-mode JSON document whose
// data is the JSON document data.
func CloudEvent(id, source, eventType, subject string, at time.Time, data []byte) (string, error) {
	body, err := json.Marshal(struct {
		SpecVersion     string          `json:"specversion"`
		ID              string          `json:"id"`
		Source          string          `json:"source"`
		Type            string          `json:"type"`
		Subject         string          `json:"subject"`
		Time            string          `json:"time"`
		DataContentType string          `json:"datacontenttype"`
		Data            json.RawMessage `json:"data"`
	}{"1.0", id, source, eventType, subject, at.UTC().Format(time.RFC3339Nano), "application/json", data})
	return string(body), err
}

func digest(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:16])
}
