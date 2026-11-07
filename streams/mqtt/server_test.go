package mqtt

import (
	"crypto/tls"
	"testing"
)

func TestParseServer(t *testing.T) {
	secure := &tls.Config{MinVersion: tls.VersionTLS12}

	cases := []struct {
		address string
		tls     *tls.Config
		want    string // empty when the address is refused
	}{
		{"localhost:1883", nil, "mqtt://localhost:1883"},
		{"localhost", nil, "mqtt://localhost:1883"},
		{"localhost", secure, "tls://localhost:8883"},
		{"broker.example:8884", secure, "tls://broker.example:8884"},
		{"mqtts://broker.example", nil, "mqtts://broker.example:8883"},
		{"wss://broker.example/mqtt", secure, "wss://broker.example/mqtt"},
		{"mqtt://localhost", secure, ""}, // TLS asked for, plain address given
		{"http://localhost", nil, ""},    // not an MQTT scheme
		{"mqtt://:1883", nil, ""},        // no host
		{"", nil, ""},
	}
	for _, tc := range cases {
		got, err := parseServer(tc.address, tc.tls)
		switch {
		case tc.want == "" && err == nil:
			t.Errorf("parseServer(%q) = %s, want it refused", tc.address, got)
		case tc.want != "" && err != nil:
			t.Errorf("parseServer(%q): %v", tc.address, err)
		case tc.want != "" && got.String() != tc.want:
			t.Errorf("parseServer(%q) = %s, want %s", tc.address, got, tc.want)
		}
	}
}
