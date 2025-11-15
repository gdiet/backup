package util_test

import (
	"path/filepath"
	"sync/atomic"
	"testing"
)

var counter atomic.Uint64

// NextId generates a unique identifier for testing purposes.
func NextId() uint64 {
	return counter.Add(7) // Don't increment by 1 to help catch off-by-one errors
}

// TempFile provides a temporary file for testing.
// The containing directory is automatically removed when the test and all its subtests complete.
func TempFile(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "testfile")
}
