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

func TestAssertf(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			expected := "assertion failed: test value 42 should be positive"
			if r != expected {
				t.Errorf("Expected panic message %q, got %q", expected, r)
			}
		} else {
			t.Error("Expected panic, but function returned normally")
		}
	}()

	// This should trigger the assertion with formatted message
	Assertf(false, "test value %d should be positive", 42)
}
