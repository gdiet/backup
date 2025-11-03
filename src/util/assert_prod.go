//go:build prod

package util

import "log"

// AssertionFailed logs a warning in production builds.
// This provides graceful degradation instead of panicking in production.
func AssertionFailed(message string) {
	log.Printf("ASSERTION FAILED: %s", message)
}

// Assert logs a warning in production builds if the condition is false.
// This provides graceful degradation instead of panicking in production.
func Assert(condition bool, message string) {
	if !condition {
		AssertionFailed(message)
	}
}

// AssertEqual logs a warning in production builds if the values are not equal.
func AssertEqual[T comparable](actual, expected T, message string) {
	Assert(actual == expected, message)
}

// AssertNotNil logs a warning in production builds if the value is nil.
func AssertNotNil[T any](value *T, message string) {
	Assert(value != nil, message)
}
