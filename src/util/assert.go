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
