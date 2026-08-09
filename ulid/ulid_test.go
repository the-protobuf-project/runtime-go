package ulid

import (
	"strings"
	"testing"
)

// GetRandomCode slices the encoded ULID at a fixed offset, so it is only
// correct as long as the timestamp prefix is exactly 10 characters.
func TestCodeLengths(t *testing.T) {
	id := Generate()

	if got := len(id.GetTimeCode()); got != 26 {
		t.Errorf("GetTimeCode length = %d, want 26", got)
	}
	if got := len(id.GetRandomCode()); got != 16 {
		t.Errorf("GetRandomCode length = %d, want 16", got)
	}
	if !strings.HasSuffix(id.GetTimeCode(), id.GetRandomCode()) {
		t.Errorf("GetRandomCode %q is not the tail of GetTimeCode %q",
			id.GetRandomCode(), id.GetTimeCode())
	}
}

// Both codes are used as Redis keys, so IDs minted back-to-back — within the
// same millisecond — must not collide.
func TestGenerateIsUniqueWithinAMillisecond(t *testing.T) {
	const n = 1000
	timeCodes := make(map[string]struct{}, n)
	randomCodes := make(map[string]struct{}, n)

	for range n {
		id := Generate()
		if _, dup := timeCodes[id.GetTimeCode()]; dup {
			t.Fatalf("duplicate time code %q", id.GetTimeCode())
		}
		if _, dup := randomCodes[id.GetRandomCode()]; dup {
			t.Fatalf("duplicate random code %q", id.GetRandomCode())
		}
		timeCodes[id.GetTimeCode()] = struct{}{}
		randomCodes[id.GetRandomCode()] = struct{}{}
	}
}

// Time codes are used where lexicographic order should mean chronological
// order — stream keys and published message IDs.
func TestTimeCodesSortChronologically(t *testing.T) {
	first := Generate().GetTimeCode()
	second := Generate().GetTimeCode()

	if first >= second {
		t.Errorf("time codes do not sort by creation order: %q >= %q", first, second)
	}
}
