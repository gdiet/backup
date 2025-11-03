package util

import "testing"

func TestAssert(t *testing.T) {
	// This should not panic in debug builds
	Assert(true, "this should pass")

	// Test AssertEqual
	AssertEqual(42, 42, "equal values should pass")

	// Test AssertNotNil
	value := 42
	AssertNotNil(&value, "non-nil pointer should pass")
}

func TestAssertPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic from false assertion")
		}
	}()

	// This should panic in debug builds
	Assert(false, "this should fail")
}
