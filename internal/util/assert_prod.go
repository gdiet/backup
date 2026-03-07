//go:build prod

package util

import (
	"fmt"
	"log/slog"
)

// AssertionFailed logs an error in production builds, then continues.
// This provides graceful degradation instead of panicking in production.
func AssertionFailed(message string) {
	slog.Error("ASSERTION FAILED", "message", message)
}

// Assert logs an error in production builds if the condition is false, then continues.
// This provides graceful degradation instead of panicking in production.
func Assert(condition bool, message string) {
	if !condition {
		AssertionFailed(message)
	}
}

// Assertf logs an error in production builds if the condition is false, with formatted message, then continues.
// This provides graceful degradation instead of panicking in production.
func Assertf(condition bool, format string, args ...interface{}) {
	if !condition {
		AssertionFailed(fmt.Sprintf(format, args...))
	}
}
