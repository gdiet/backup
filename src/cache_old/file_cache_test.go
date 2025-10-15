package cache_old

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewFileCache(t *testing.T) {
	tempDir := t.TempDir()

	cache, err := NewFileCache(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileCache: %v", err)
	}
	defer cache.Close()

	// Verify the cache was created properly
	if cache.baseDir != tempDir {
		t.Errorf("Expected baseDir %q, got %q", tempDir, cache.baseDir)
	}

	// Verify directory exists
	if _, err := os.Stat(tempDir); os.IsNotExist(err) {
		t.Errorf("Cache directory was not created: %s", tempDir)
	}
}

func TestFileCache_BasicOperations(t *testing.T) {
	tempDir := t.TempDir()
	cache, err := NewFileCache(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileCache: %v", err)
	}
	defer cache.Close()

	fileId := 1
	testData := []byte("Hello, FileCache!")

	// Test Write
	err = cache.Write(fileId, 0, testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Test Read
	readData, err := cache.Read(fileId, 0, len(testData))
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !bytes.Equal(testData, readData) {
		t.Errorf("Data mismatch: expected %q, got %q", testData, readData)
	}

	// Verify file was created on disk
	filePath := filepath.Join(tempDir, fmt.Sprintf("%d", fileId))
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Errorf("File was not created on disk: %s", filePath)
	}

	// Test Length method
	length, err := cache.Length(fileId)
	if err != nil {
		t.Fatalf("Length failed: %v", err)
	}
	if length != int64(len(testData)) {
		t.Errorf("Length mismatch: expected %d, got %d", len(testData), length)
	}
}

func TestFileCache_Length(t *testing.T) {
	tempDir := t.TempDir()
	cache, err := NewFileCache(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileCache: %v", err)
	}
	defer cache.Close()

	fileId := 2

	// Test Length on non-existent file (should create empty file)
	length, err := cache.Length(fileId)
	if err != nil {
		t.Fatalf("Length on new file failed: %v", err)
	}
	if length != 0 {
		t.Errorf("New file should have length 0, got %d", length)
	}

	// Write some data and check length
	testData := []byte("Hello, Length Test!")
	err = cache.Write(fileId, 0, testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	length, err = cache.Length(fileId)
	if err != nil {
		t.Fatalf("Length after write failed: %v", err)
	}
	if length != int64(len(testData)) {
		t.Errorf("Length after write: expected %d, got %d", len(testData), length)
	}

	// Write at a later position and check length
	moreData := []byte(" More data")
	writePos := int64(len(testData) + 10) // Leave a gap
	err = cache.Write(fileId, writePos, moreData)
	if err != nil {
		t.Fatalf("Write at later position failed: %v", err)
	}

	expectedLength := writePos + int64(len(moreData))
	length, err = cache.Length(fileId)
	if err != nil {
		t.Fatalf("Length after gap write failed: %v", err)
	}
	if length != expectedLength {
		t.Errorf("Length after gap write: expected %d, got %d", expectedLength, length)
	}

	// Truncate and check length
	truncateLen := int64(10)
	err = cache.Truncate(fileId, truncateLen)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	length, err = cache.Length(fileId)
	if err != nil {
		t.Fatalf("Length after truncate failed: %v", err)
	}
	if length != truncateLen {
		t.Errorf("Length after truncate: expected %d, got %d", truncateLen, length)
	}

	// Extend with truncate and check length
	extendLen := int64(50)
	err = cache.Truncate(fileId, extendLen)
	if err != nil {
		t.Fatalf("Extend truncate failed: %v", err)
	}

	length, err = cache.Length(fileId)
	if err != nil {
		t.Fatalf("Length after extend failed: %v", err)
	}
	if length != extendLen {
		t.Errorf("Length after extend: expected %d, got %d", extendLen, length)
	}
}

func TestFileCache_Truncate(t *testing.T) {
	tempDir := t.TempDir()
	cache, err := NewFileCache(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileCache: %v", err)
	}
	defer cache.Close()

	fileId := 3
	testData := []byte("This is a longer test string for truncation")

	// Write initial data
	err = cache.Write(fileId, 0, testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Truncate to shorter length
	newLength := int64(10)
	err = cache.Truncate(fileId, newLength)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Read the truncated data
	readData, err := cache.Read(fileId, 0, len(testData))
	if err != nil {
		t.Fatalf("Read after truncate failed: %v", err)
	}

	expectedData := testData[:newLength]
	if !bytes.Equal(expectedData, readData) {
		t.Errorf("Truncated data mismatch: expected %q, got %q", expectedData, readData)
	}

	// Test truncate to extend file (should fill with zeros)
	extendLength := int64(20)
	err = cache.Truncate(fileId, extendLength)
	if err != nil {
		t.Fatalf("Extend truncate failed: %v", err)
	}

	// Read extended data
	extendedData, err := cache.Read(fileId, 0, int(extendLength))
	if err != nil {
		t.Fatalf("Read after extend failed: %v", err)
	}

	if int64(len(extendedData)) != extendLength {
		t.Errorf("Extended data length mismatch: expected %d, got %d", extendLength, len(extendedData))
	}

	// Check that the first part is still the original truncated data
	if !bytes.Equal(expectedData, extendedData[:newLength]) {
		t.Errorf("Original data was corrupted after extend")
	}
}

func TestFileCache_RandomAccess(t *testing.T) {
	tempDir := t.TempDir()
	cache, err := NewFileCache(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileCache: %v", err)
	}
	defer cache.Close()

	fileId := 4

	// Write data at different positions
	data1 := []byte("AAAA")
	data2 := []byte("BBBB")
	data3 := []byte("CCCC")

	err = cache.Write(fileId, 0, data1)
	if err != nil {
		t.Fatalf("Write at position 0 failed: %v", err)
	}

	err = cache.Write(fileId, 10, data2)
	if err != nil {
		t.Fatalf("Write at position 10 failed: %v", err)
	}

	err = cache.Write(fileId, 20, data3)
	if err != nil {
		t.Fatalf("Write at position 20 failed: %v", err)
	}

	// Read data back from different positions
	read1, err := cache.Read(fileId, 0, 4)
	if err != nil {
		t.Fatalf("Read at position 0 failed: %v", err)
	}
	if !bytes.Equal(data1, read1) {
		t.Errorf("Data1 mismatch: expected %q, got %q", data1, read1)
	}

	read2, err := cache.Read(fileId, 10, 4)
	if err != nil {
		t.Fatalf("Read at position 10 failed: %v", err)
	}
	if !bytes.Equal(data2, read2) {
		t.Errorf("Data2 mismatch: expected %q, got %q", data2, read2)
	}

	read3, err := cache.Read(fileId, 20, 4)
	if err != nil {
		t.Fatalf("Read at position 20 failed: %v", err)
	}
	if !bytes.Equal(data3, read3) {
		t.Errorf("Data3 mismatch: expected %q, got %q", data3, read3)
	}

	// Read gap area (should be zeros)
	gapData, err := cache.Read(fileId, 4, 6)
	if err != nil {
		t.Fatalf("Read gap failed: %v", err)
	}
	expectedGap := make([]byte, 6)
	if !bytes.Equal(expectedGap, gapData) {
		t.Errorf("Gap data should be zeros: expected %v, got %v", expectedGap, gapData)
	}
}

func TestFileCache_Dispose(t *testing.T) {
	tempDir := t.TempDir()
	cache, err := NewFileCache(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileCache: %v", err)
	}
	defer cache.Close()

	fileId := 5
	testData := []byte("Data to be disposed")

	// Write some data
	err = cache.Write(fileId, 0, testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Verify file exists
	filePath := filepath.Join(tempDir, fmt.Sprintf("%d", fileId))
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Errorf("File should exist before dispose: %s", filePath)
	}

	// Dispose the file
	err = cache.Dispose(fileId)
	if err != nil {
		t.Fatalf("Dispose failed: %v", err)
	}

	// Verify file was deleted
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Errorf("File should not exist after dispose: %s", filePath)
	}

	// Verify we can still create a new file with the same ID
	err = cache.Write(fileId, 0, []byte("New data"))
	if err != nil {
		t.Fatalf("Write after dispose failed: %v", err)
	}
}

func TestFileCache_MultipleFiles(t *testing.T) {
	tempDir := t.TempDir()
	cache, err := NewFileCache(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileCache: %v", err)
	}
	defer cache.Close()

	// Create multiple files
	files := map[int][]byte{
		10: []byte("Content of file 1"),
		20: []byte("Content of file 2"),
		30: []byte("Content of file 3"),
	}

	// Write all files
	for fileId, data := range files {
		err = cache.Write(fileId, 0, data)
		if err != nil {
			t.Fatalf("Write to %d failed: %v", fileId, err)
		}
	}

	// Read all files back
	for fileId, expectedData := range files {
		readData, err := cache.Read(fileId, 0, len(expectedData))
		if err != nil {
			t.Fatalf("Read from %d failed: %v", fileId, err)
		}

		if !bytes.Equal(expectedData, readData) {
			t.Errorf("Data mismatch for %d: expected %q, got %q", fileId, expectedData, readData)
		}
	}

	// Get stats
	stats := cache.GetStats()
	if stats["numberOfFiles"].(int) != len(files) {
		t.Errorf("Expected %d open files, got %d", len(files), stats["numberOfFiles"].(int))
	}
}

func TestFileCache_ConcurrentAccess(t *testing.T) {
	tempDir := t.TempDir()
	cache, err := NewFileCache(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileCache: %v", err)
	}
	defer cache.Close()

	fileId := 6
	numWorkers := 10
	numOperations := 100

	// Channel to collect errors
	errors := make(chan error, numWorkers*numOperations)
	done := make(chan bool, numWorkers)

	// Start concurrent workers
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer func() { done <- true }()

			for j := 0; j < numOperations; j++ {
				data := []byte{byte(workerID), byte(j)}
				position := int64(workerID*1000 + j*2)

				// Write
				if err := cache.Write(fileId, position, data); err != nil {
					errors <- err
					return
				}

				// Read back
				readData, err := cache.Read(fileId, position, 2)
				if err != nil {
					errors <- err
					return
				}

				if !bytes.Equal(data, readData) {
					errors <- fmt.Errorf("worker %d, op %d: data mismatch", workerID, j)
					return
				}
			}
		}(i)
	}

	// Wait for all workers with timeout
	timeout := time.After(30 * time.Second)
	workersFinished := 0

	for workersFinished < numWorkers {
		select {
		case <-done:
			workersFinished++
		case err := <-errors:
			t.Fatalf("Concurrent access error: %v", err)
		case <-timeout:
			t.Fatalf("Test timed out waiting for workers to complete")
		}
	}

	// Check for any remaining errors
	select {
	case err := <-errors:
		t.Fatalf("Concurrent access error: %v", err)
	default:
		// No errors
	}
}

func TestFileCache_ErrorConditions(t *testing.T) {
	tempDir := t.TempDir()
	cache, err := NewFileCache(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileCache: %v", err)
	}
	defer cache.Close()

	fileId := 7

	// Test negative position for read
	_, err = cache.Read(fileId, -1, 10)
	if err == nil {
		t.Error("Expected error for negative position in Read")
	}

	// Test negative length for read
	_, err = cache.Read(fileId, 0, -1)
	if err == nil {
		t.Error("Expected error for negative length in Read")
	}

	// Test negative position for write
	err = cache.Write(fileId, -1, []byte("test"))
	if err == nil {
		t.Error("Expected error for negative position in Write")
	}

	// Test negative length for truncate
	err = cache.Truncate(fileId, -1)
	if err == nil {
		t.Error("Expected error for negative length in Truncate")
	}

	// Test zero-length operations (should not error)
	_, err = cache.Read(fileId, 0, 0)
	if err != nil {
		t.Errorf("Zero-length read should not error: %v", err)
	}

	err = cache.Write(fileId, 0, []byte{})
	if err != nil {
		t.Errorf("Zero-length write should not error: %v", err)
	}
}

func TestFileCache_InternalConsistency(t *testing.T) {
	tempDir := t.TempDir()
	cache, err := NewFileCache(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileCache: %v", err)
	}
	defer cache.Close()

	// Test that files are tracked correctly
	testFiles := []int{100, 200, 300}

	// Create files and check consistency
	for i, fileId := range testFiles {
		err = cache.Write(fileId, 0, []byte("test data"))
		if err != nil {
			t.Fatalf("Failed to write to %d: %v", fileId, err)
		}

		// Check that the correct number of files are tracked
		stats := cache.GetStats()
		numberOfFiles := stats["numberOfFiles"].(int)
		expectedFiles := i + 1 // We've created i+1 files so far

		if numberOfFiles != expectedFiles {
			t.Errorf("Expected %d files, got %d", expectedFiles, numberOfFiles)
		}
	}

	// Verify final state
	stats := cache.GetStats()
	if stats["numberOfFiles"].(int) != len(testFiles) {
		t.Errorf("Expected %d open files, got %d", len(testFiles), stats["numberOfFiles"].(int))
	}

	// Dispose files and check consistency
	for i, fileId := range testFiles {
		err = cache.Dispose(fileId)
		if err != nil {
			t.Fatalf("Failed to dispose %d: %v", fileId, err)
		}

		// Check consistency after each disposal
		stats := cache.GetStats()
		numberOfFiles := stats["numberOfFiles"].(int)
		expectedFiles := len(testFiles) - (i + 1) // i+1 files have been disposed

		if numberOfFiles != expectedFiles {
			t.Errorf("After disposing %d files, expected %d remaining, got %d", i+1, expectedFiles, numberOfFiles)
		}
	}

	// Final state should be empty
	stats = cache.GetStats()
	if stats["numberOfFiles"].(int) != 0 {
		t.Errorf("Expected 0 open files after dispose all, got %d", stats["numberOfFiles"].(int))
	}
	if stats["numberOfFiles"].(int) != 0 {
		t.Errorf("Expected 0 tracked files after dispose all, got %d", stats["numberOfFiles"].(int))
	}
}

func TestFileCache_InvalidDirectory(t *testing.T) {
	// Try to create cache in a location that can't be created
	invalidDir := "/root/non-existent-path-that-cannot-be-created"
	_, err := NewFileCache(invalidDir)
	if err == nil {
		t.Error("Expected error when creating cache in invalid directory")
	}
}

func TestFileCache_Close(t *testing.T) {
	tempDir := t.TempDir()
	cache, err := NewFileCache(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileCache: %v", err)
	}

	// Create some files
	for i := 0; i < 3; i++ {
		fileId := i + 1000
		err = cache.Write(fileId, 0, []byte("test data"))
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// Verify files are open
	stats := cache.GetStats()
	if stats["numberOfFiles"].(int) != 3 {
		t.Errorf("Expected 3 open files, got %d", stats["numberOfFiles"].(int))
	}

	// Close the cache
	err = cache.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify all files are closed
	stats = cache.GetStats()
	if stats["numberOfFiles"].(int) != 0 {
		t.Errorf("Expected 0 open files after close, got %d", stats["numberOfFiles"].(int))
	}
}

// Benchmark tests
func BenchmarkFileCache_Write(b *testing.B) {
	tempDir := b.TempDir()
	cache, err := NewFileCache(tempDir)
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	data := make([]byte, 1024) // 1KB
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		fileId := i%100 + 2000 // Reuse file IDs
		position := int64(i % 1000 * 1024)
		err := cache.Write(fileId, position, data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileCache_Read(b *testing.B) {
	tempDir := b.TempDir()
	cache, err := NewFileCache(tempDir)
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	// Pre-populate with data
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	for i := 0; i < 100; i++ {
		fileId := i + 3000
		for j := 0; j < 10; j++ {
			err := cache.Write(fileId, int64(j*1024), data)
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		fileId := i%100 + 4000
		position := int64(i % 10 * 1024)
		_, err := cache.Read(fileId, position, 1024)
		if err != nil {
			b.Fatal(err)
		}
	}
}
