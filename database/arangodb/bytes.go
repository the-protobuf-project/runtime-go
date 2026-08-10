package arangodb

import (
	"encoding/base64"
	"strings"
)

// bytesPrefix marks a base64 string as having been a []byte rather than text.
//
// Without it a read cannot tell a blob from a caller's own string that happens
// to be valid base64 — "dGVzdA==" is a perfectly ordinary thing for someone to
// store — and would hand back bytes where a string was written. The prefix is
// not valid base64, so it cannot occur by accident in the encoded form.
const bytesPrefix = "$bin:"

// encodeBytes renders a blob for a JSON document.
func encodeBytes(b []byte) string {
	return bytesPrefix + base64.StdEncoding.EncodeToString(b)
}

// decodeBytes reverses [encodeBytes], reporting whether the value was one.
func decodeBytes(s string) ([]byte, bool) {
	raw, ok := strings.CutPrefix(s, bytesPrefix)
	if !ok {
		return nil, false
	}
	b, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, false
	}
	return b, true
}
