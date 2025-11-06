//go:build prod

package util

import (
	"fmt"
	"log"
)

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

// Assertf logs a warning in production builds if the condition is false, with formatted message.
// This provides graceful degradation instead of panicking in production.
func Assertf(condition bool, format string, args ...interface{}) {
	if !condition {
		AssertionFailed(fmt.Sprintf(format, args...))
	}
}
