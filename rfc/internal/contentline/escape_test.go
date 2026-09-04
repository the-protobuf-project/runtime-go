// Copyright 2026 The Protobuf Project authors.
// SPDX-License-Identifier: Apache-2.0

package contentline

import (
	"strings"
	"testing"
	"time"
)

// TestFoldTerminatesOnMalformedUTF8 pins the loop-progress guard.
//
// Fold walks back from the 75-octet limit to a rune boundary so a multi-byte
// sequence is not split. Where the whole window is continuation bytes there is
// no boundary to find, and the walk used to reach start, write nothing and
// leave start unchanged -- an infinite loop reachable from encoding any value
// carrying such a run.
func TestFoldTerminatesOnMalformedUTF8(t *testing.T) {
	for name, in := range map[string]string{
		"all continuation bytes": strings.Repeat("\x80", 200),
		"leading run then ascii": strings.Repeat("\x80", 100) + strings.Repeat("a", 100),
		"valid utf-8":            strings.Repeat("é", 100),
	} {
		t.Run(name, func(t *testing.T) {
			done := make(chan string, 1)
			go func() { done <- Fold(in) }()
			select {
			case got := <-done:
				// Folding must be lossless: the octets are unchanged, only
				// CRLF-space separators are inserted.
				if stripped := strings.ReplaceAll(got, "\r\n ", ""); stripped != in {
					t.Errorf("Fold lost or altered octets: %d in, %d out", len(in), len(stripped))
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Fold did not terminate: the loop made no progress")
			}
		})
	}
}

// TestEscapeParamRefusesUnrepresentable pins RFC 5545 section 3.1: a parameter
// value "MUST NOT contain the DQUOTE character", and CR or LF would end the
// content line so the remainder parses as a separate property. Neither has an
// escape form, so both are refused rather than dropped.
func TestEscapeParamRefusesUnrepresentable(t *testing.T) {
	for name, in := range map[string]string{
		"dquote":            `say "hello"`,
		"carriage return":   "a\rb",
		"line feed":         "a\nb",
		"injected property": "x\r\nSUMMARY:injected",
	} {
		t.Run(name, func(t *testing.T) {
			if got, err := EscapeParam(in); err == nil {
				t.Errorf("EscapeParam(%q) = %q, want an error", in, got)
			}
		})
	}

	// And the ordinary cases still work.
	for in, want := range map[string]string{
		"work":       "work",
		"a,b":        `"a,b"`,
		"http://x/y": `"http://x/y"`,
	} {
		got, err := EscapeParam(in)
		if err != nil {
			t.Errorf("EscapeParam(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("EscapeParam(%q) = %q, want %q", in, got, want)
		}
	}
}
