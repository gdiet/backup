package cache

import (
	"sync"
	"testing"
	"time"
)

func TestFileCache_ConcurrentDispose(t *testing.T) {
	// Create cache
	fc, err := NewFileCache("/tmp/concurrent-dispose-test")
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer fc.Close()

	fileId := 42

	// Write some data to create the file
	err = fc.Write(fileId, 0, []byte("Test data for concurrent dispose"))
	if err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 5)

	// Start 5 concurrent Dispose operations on the same fileId
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(threadId int) {
			defer wg.Done()

			// Add some randomness to increase chance of race
			time.Sleep(time.Duration(threadId*2) * time.Millisecond)

			err := fc.Dispose(fileId)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check that no errors occurred
	for err := range errors {
		t.Errorf("Concurrent dispose failed: %v", err)
	}

	// Check cache state immediately after dispose
	stats := fc.GetStats()
	openFiles := stats["openFiles"].(int)
	trackedFiles := stats["trackedFiles"].(int)

	if openFiles != 0 {
		t.Errorf("Expected 0 open files after dispose, got %d", openFiles)
	}
	if trackedFiles != 0 {
		t.Errorf("Expected 0 tracked files after dispose, got %d", trackedFiles)
	}

	// Verify file can be accessed again (lazy loading) - this should succeed
	// The physical file was deleted, so reading should return empty data
	data, err := fc.Read(fileId, 0, 10)
	if err != nil {
		t.Errorf("Failed to read from recreated file: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("Expected empty data from new file, got %d bytes", len(data))
	}
}

func TestFileCache_DisposeNonExistent(t *testing.T) {
	// Create cache
	fc, err := NewFileCache("/tmp/dispose-nonexistent-test")
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer fc.Close()

	// Try to dispose a file that doesn't exist
	err = fc.Dispose(999)
	if err != nil {
		t.Errorf("Dispose of non-existent file should succeed (idempotent), got error: %v", err)
	}

	// Try multiple times - should all succeed
	for i := 0; i < 3; i++ {
		err = fc.Dispose(999)
		if err != nil {
			t.Errorf("Multiple dispose calls should succeed (idempotent), got error: %v", err)
		}
	}
}
