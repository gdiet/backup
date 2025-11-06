//go:build !prod

package util

import "fmt"

// AssertionFailed in debug builds panics with the given message.
// This helps catch programming errors during development.
// In production builds, assertions are disabled for better performance.
func AssertionFailed(message string) {
	panic("assertion failed: " + message)
}

// Assert panics in debug builds if the condition is false.
// This helps catch programming errors during development.
// In production builds, assertions are disabled for better performance.
func Assert(condition bool, message string) {
	if !condition {
		AssertionFailed(message)
	}
}

// Assertf panics in debug builds if the condition is false, with formatted message.
// This helps catch programming errors during development.
// In production builds, assertions are disabled for better performance.
func Assertf(condition bool, format string, args ...interface{}) {
	if !condition {
		AssertionFailed(fmt.Sprintf(format, args...))
	}
}
