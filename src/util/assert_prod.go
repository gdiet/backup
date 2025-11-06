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
