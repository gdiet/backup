package util

import "testing"

func TestAssertPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic from false assertion")
		}
	}()

	// This should panic in debug builds
	Assert(false, "this should fail")
}
