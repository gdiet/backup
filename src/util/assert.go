//go:build !prod

package util

// AssertionFailed in debug builds panics with the given message.
// This helps catch programming errors during development.
// In production builds, assertions are disabled for performance.
func AssertionFailed(message string) {
	panic("assertion failed: " + message)
}

// Assert panics in debug builds if the condition is false.
// This helps catch programming errors during development.
// In production builds, assertions are disabled for performance.
func Assert(condition bool, message string) {
	if !condition {
		AssertionFailed(message)
	}
}

// AssertEqual panics in debug builds if the values are not equal.
func AssertEqual[T comparable](actual, expected T, message string) {
	Assert(actual == expected, message)
}

// AssertNotNil panics in debug builds if the value is nil.
func AssertNotNil[T any](value *T, message string) {
	Assert(value != nil, message)
}
