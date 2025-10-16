package cache_old2

// Test helper functions for Memory testing.
// These functions are only compiled when running tests (due to _test.go suffix naming convention).

// calculateMemoryUsage returns the total bytes allocated by all data areas in memory.
// This is a test helper function for validating memory usage changes in tests.
func (memory *Memory) calculateMemoryUsage() int {
	totalBytes := 0
	for _, area := range memory.areas {
		totalBytes += len(area.Data)
	}
	return totalBytes
}
