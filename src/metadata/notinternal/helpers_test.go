package internal

import (
	"testing"
)

func TestU64bB64uRoundtrip(t *testing.T) {
	tests := []uint64{0, 1, 42, 9223372036854775807, 18446744073709551615}

	for _, test := range tests {
		key := U64b(test)
		if len(key) != 8 {
			t.Errorf("Key for %d has wrong length: expected 8, got %d", test, len(key))
		}

		// Test round-trip conversion
		restored := B64u(key)
		if restored != test {
			t.Errorf("Round-trip failed for %d: got %d", test, restored)
		}
	}
}
