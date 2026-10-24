package events

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"
)

// The Standard Webhooks headers every delivery carries.
const (
	HeaderID        = "webhook-id"
	HeaderTimestamp = "webhook-timestamp"
	HeaderSignature = "webhook-signature"
)

// SecretPrefix starts a Standard Webhooks signing secret.
const SecretPrefix = "whsec_"

// ErrUnreadableSecret: a signing secret is not "whsec_" and base64.
var ErrUnreadableSecret = errors.New("the webhook's signing secret is unreadable")

// NewSecret is a fresh signing secret in the Standard Webhooks form:
// "whsec_" and the base64 of 32 random bytes.
func NewSecret() (string, error) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return "", err
	}
	return SecretPrefix + base64.StdEncoding.EncodeToString(key), nil
}

// Sign is one Standard Webhooks signature, "v1,<base64 HMAC-SHA256>", of
// "<id>.<timestamp>.<body>" under secret.
func Sign(secret, id string, at time.Time, body []byte) (string, error) {
	return sign(secret, id+"."+strconv.FormatInt(at.Unix(), 10)+".", body)
}

func sign(secret, prefix string, body []byte) (string, error) {
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, SecretPrefix))
	if err != nil || len(key) == 0 {
		return "", ErrUnreadableSecret
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(prefix))
	mac.Write(body)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}

// Verify reports whether signatures -- a webhook-signature header, one or
// more space-separated "v1,..." values -- holds a signature of body under
// secret, as a receiver checks it. Checking the timestamp's age is the
// caller's.
func Verify(secret, id, timestamp string, body []byte, signatures string) bool {
	want, err := sign(secret, id+"."+timestamp+".", body)
	if err != nil {
		return false
	}
	for _, got := range strings.Fields(signatures) {
		if hmac.Equal([]byte(got), []byte(want)) {
			return true
		}
	}
	return false
}
