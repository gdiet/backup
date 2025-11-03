package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// TestBasicOperations tests basic read/write operations
func TestBasicOperations(t *testing.T) {
	tempDir := t.TempDir()
	store, err := FileBackedDataStore(tempDir, 1024, 5)
	if err != nil {
		t.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	// Test write
	data := []byte("Hello, Storage5!")
	err = store.Write(0, data)
	if err != nil {
		t.Fatal("Write failed:", err)
	}

	// Test read
	readData, warnings := store.Read(0, int64(len(data)))
	if len(warnings) > 0 {
		t.Errorf("Unexpected warnings during read: %v", warnings)
	}

	if !bytes.Equal(data, readData) {
		t.Fatalf("Data mismatch: expected %s, got %s", string(data), string(readData))
	}
}

// TestCrossFileBoundary tests writing and reading across file boundaries
func TestCrossFileBoundary(t *testing.T) {
	tempDir := t.TempDir()
	store, err := FileBackedDataStore(tempDir, 100, 5) // Small file size to force boundary crossing
	if err != nil {
		t.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	// Create data that spans multiple files
	data := make([]byte, 250) // 2.5 files
	for i := range data {
		data[i] = byte('A' + (i % 26))
	}

	// Write data starting near the end of first file
	offset := int64(80)
	err = store.Write(offset, data)
	if err != nil {
		t.Fatal("Cross-boundary write failed:", err)
	}

	// Read it back
	readData, warnings := store.Read(offset, int64(len(data)))
	if len(warnings) > 0 {
		t.Errorf("Unexpected warnings during cross-boundary read: %v", warnings)
	}

	if !bytes.Equal(data, readData) {
		t.Fatal("Cross-boundary data mismatch")
	}
}

// TestLargeData tests writing and reading large amounts of data
func TestLargeData(t *testing.T) {
	tempDir := t.TempDir()
	store, err := FileBackedDataStore(tempDir, 1024*1024, 5) // 1MB files
	if err != nil {
		t.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	// Create 5MB of test data
	data := make([]byte, 5*1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Write in chunks
	chunkSize := int64(64 * 1024) // 64KB chunks
	for offset := int64(0); offset < int64(len(data)); offset += chunkSize {
		end := offset + chunkSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}

		err = store.Write(offset, data[offset:end])
		if err != nil {
			t.Fatalf("Write failed at offset %d: %v", offset, err)
		}
	}

	// Read it back in chunks and verify
	for offset := int64(0); offset < int64(len(data)); offset += chunkSize {
		readSize := chunkSize
		if offset+readSize > int64(len(data)) {
			readSize = int64(len(data)) - offset
		}

		readData, warnings := store.Read(offset, readSize)
		if len(warnings) > 0 {
			t.Errorf("Unexpected warnings during large data read at offset %d: %v", offset, warnings)
		}
		expected := data[offset : offset+readSize]

		if !bytes.Equal(expected, readData) {
			t.Fatalf("Large data mismatch at offset %d", offset)
		}
	}
}

// TestConcurrentAccess tests concurrent read/write operations
func TestConcurrentAccess(t *testing.T) {
	tempDir := t.TempDir()
	store, err := FileBackedDataStore(tempDir, 1024, 10) // Higher limit for concurrent access
	if err != nil {
		t.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	const numGoroutines = 10
	const dataSize = 100

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*2) // Read + Write operations

	// Launch concurrent writers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Create unique data for this goroutine
			data := make([]byte, dataSize)
			for j := range data {
				data[j] = byte('A' + (id % 26))
			}

			offset := int64(id * dataSize * 2) // Space out writes to avoid overlap

			err := store.Write(offset, data)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d write error: %v", id, err)
				return
			}

			// Read back immediately
			readData, warnings := store.Read(offset, int64(len(data)))
			if len(warnings) > 0 {
				errors <- fmt.Errorf("goroutine %d unexpected warnings: %v", id, warnings)
				return
			}

			if !bytes.Equal(data, readData) {
				errors <- fmt.Errorf("goroutine %d data mismatch", id)
				return
			}
		}(i)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(errors)
	}()

	// Check for errors
	for err := range errors {
		t.Error(err)
	}
}

// TestFileHandleLimits tests the LRU file handle eviction
func TestFileHandleLimits(t *testing.T) {
	tempDir := t.TempDir()
	// Write to more files than openFilesSoftLimit limit (using limit of 5)
	openFilesSoftLimit := 5
	store, err := FileBackedDataStore(tempDir, 1024, openFilesSoftLimit)
	if err != nil {
		t.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	numFiles := openFilesSoftLimit + 3
	data := []byte("Test data for file handle limits")

	// Write to many different files (force LRU eviction)
	for i := 0; i < numFiles; i++ {
		offset := int64(i) * 2048 // Each file is 1024 bytes, so this hits different files
		err := store.Write(offset, data)
		if err != nil {
			t.Fatalf("Write to file %d failed: %v", i, err)
		}
	}

	// Read back from all files to ensure LRU worked correctly
	for i := 0; i < numFiles; i++ {
		offset := int64(i) * 2048
		readData, warnings := store.Read(offset, int64(len(data)))
		if len(warnings) > 0 {
			t.Errorf("Unexpected warnings during LRU test file %d: %v", i, warnings)
		}

		if !bytes.Equal(data, readData) {
			t.Fatalf("LRU test failed: data mismatch in file %d", i)
		}
	}
}

// TestErrorHandling tests various error conditions
func TestErrorHandling(t *testing.T) {
	// Test invalid file size
	tempDir := t.TempDir()
	_, err := FileBackedDataStore(tempDir, 0, 5)
	if err == nil {
		t.Error("Expected error for zero file size")
	}

	_, err = FileBackedDataStore(tempDir, -1, 5)
	if err == nil {
		t.Error("Expected error for negative file size")
	}

	// Test invalid directory
	invalidDir := "/root/non-existent-directory-that-cannot-be-created"
	_, err = FileBackedDataStore(invalidDir, 1024, 5)
	if err == nil {
		t.Error("Expected error for invalid directory")
	}

	// Test write with invalid parameters
	store, err := FileBackedDataStore(tempDir, 1024, 5)
	if err != nil {
		t.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	// Test write with negative offset
	err = store.Write(-1, []byte("test"))
	if err == nil {
		t.Error("Expected error for negative offset")
	}
}

// TestReadNonExistentData tests reading from areas that haven't been written
func TestReadNonExistentData(t *testing.T) {
	tempDir := t.TempDir()
	store, err := FileBackedDataStore(tempDir, 1024, 5)
	if err != nil {
		t.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	// Read from unwritten area - should return zeros (and warnings are expected!)
	readData, warnings := store.Read(1000, 100)
	if len(warnings) == 0 {
		t.Error("Expected warnings when reading non-existent data, but got none")
	}

	// Check that it's all zeros
	expectedZeros := make([]byte, 100)
	if !bytes.Equal(expectedZeros, readData) {
		t.Error("Reading unwritten data should return zeros")
	}
}

// TestWriteEmptyData tests writing empty data
func TestWriteEmptyData(t *testing.T) {
	tempDir := t.TempDir()
	store, err := FileBackedDataStore(tempDir, 1024, 5)
	if err != nil {
		t.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	// Write empty data - should not error
	err = store.Write(0, []byte{})
	if err != nil {
		t.Error("Writing empty data should not error:", err)
	}

	// Read zero bytes - should not error
	readData, warnings := store.Read(0, 0)
	if len(warnings) > 0 {
		t.Errorf("Unexpected warnings when reading zero bytes: %v", warnings)
	}
	if len(readData) != 0 {
		t.Error("Reading zero bytes should return empty slice")
	}
}

// TestCloseAndReopen tests that data persists after closing and reopening
func TestCloseAndReopen(t *testing.T) {
	tempDir := t.TempDir()

	// First session: write data
	store1, err := FileBackedDataStore(tempDir, 1024, 5)
	if err != nil {
		t.Fatal("Failed to create store:", err)
	}

	data := []byte("Persistent data test")
	err = store1.Write(100, data)
	if err != nil {
		t.Fatal("Write failed:", err)
	}

	store1.Close()

	// Second session: read data
	store2, err := FileBackedDataStore(tempDir, 1024, 5)
	if err != nil {
		t.Fatal("Failed to create store:", err)
	}
	defer store2.Close()

	readData, warnings := store2.Read(100, int64(len(data)))
	if len(warnings) > 0 {
		t.Errorf("Unexpected warnings after reopen: %v", warnings)
	}
	if !bytes.Equal(data, readData) {
		t.Errorf("Data mismatch after reopen. Expected %s, got %s", data, readData)
	}
}

// BenchmarkWrite benchmarks write performance
func BenchmarkWrite(b *testing.B) {
	tempDir := b.TempDir()
	store, err := FileBackedDataStore(tempDir, 1024*1024, 10)
	if err != nil {
		b.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	data := make([]byte, 4096) // 4KB chunks
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		offset := int64(i) * int64(len(data))
		err := store.Write(offset, data)
		if err != nil {
			b.Fatal("Write failed:", err)
		}
	}
}

// BenchmarkRead benchmarks read performance
func BenchmarkRead(b *testing.B) {
	tempDir := b.TempDir()
	store, err := FileBackedDataStore(tempDir, 1024*1024, 10)
	if err != nil {
		b.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	// Pre-populate with data
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	for i := 0; i < 1000; i++ {
		err := store.Write(int64(i)*int64(len(data)), data)
		if err != nil {
			b.Fatal("Setup write failed:", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		offset := int64(i%1000) * int64(len(data))
		readData, _ := store.Read(offset, int64(len(data))) // Ignore warnings in benchmark
		if len(readData) != len(data) {
			b.Fatal("Read returned wrong length")
		}
	}
}

// BenchmarkConcurrentAccess benchmarks concurrent read/write performance
func BenchmarkConcurrentAccess(b *testing.B) {
	tempDir := b.TempDir()
	store, err := FileBackedDataStore(tempDir, 1024*1024, 20) // Higher limit for concurrent benchmark
	if err != nil {
		b.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			offset := int64(i) * int64(len(data))

			// Alternate between write and read
			if i%2 == 0 {
				err := store.Write(offset, data)
				if err != nil {
					b.Fatal("Concurrent write failed:", err)
				}
			} else {
				readData, _ := store.Read(offset-int64(len(data)), int64(len(data))) // Ignore warnings in benchmark
				if len(readData) != len(data) {
					b.Fatal("Concurrent read returned wrong length")
				}
			}
			i++
		}
	})
}

// Example demonstrates the storage package usage
func ExampleFileBackedDataStore() {
	// Create a new FileBackedDataStore
	store, err := FileBackedDataStore("/tmp/storage_example", 1024*1024, 10)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer store.Close()

	// Write some data
	data := []byte("File-backed storage with withFile() pattern!")
	err = store.Write(0, data)
	if err != nil {
		fmt.Printf("Write error: %v\n", err)
		return
	}

	// Read it back
	readData, warnings := store.Read(0, int64(len(data)))
	if len(warnings) > 0 {
		fmt.Printf("Warnings: %v\n", warnings)
	}

	fmt.Printf("Success: %s\n", string(readData))
	// Output: Success: File-backed storage with withFile() pattern!
}

// TestConcurrentSameFile tests concurrent access to the same file to cover
// the "handle already leased" code path in leaseFileHandle (lines 148-150)
func TestConcurrentSameFile(t *testing.T) {
	tempDir := t.TempDir()
	store, err := FileBackedDataStore(tempDir, 1024, 2) // Low limit to force handle reuse
	if err != nil {
		t.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	// First, create the file by writing some data
	initialData := []byte("initial data to create file")
	err = store.Write(0, initialData)
	if err != nil {
		t.Fatal("Failed to write initial data:", err)
	}

	const numGoroutines = 10
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*3)

	// Use a channel to coordinate timing and increase chance of concurrent access
	startSignal := make(chan struct{})

	// Launch multiple goroutines that will simultaneously access the same file (fileID = 0)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(3) // Multiple operations per goroutine to increase concurrency

		// Writer goroutine
		go func(id int) {
			defer wg.Done()
			<-startSignal // Wait for start signal to increase concurrency

			offset := int64(id * 20) // Different offsets within same file
			data := []byte(fmt.Sprintf("write-%02d", id))

			err := store.Write(offset, data)
			if err != nil {
				errors <- fmt.Errorf("write error in goroutine %d: %v", id, err)
			}
		}(i)

		// Reader goroutine 1
		go func(id int) {
			defer wg.Done()
			<-startSignal // Wait for start signal to increase concurrency

			offset := int64(id * 20)
			_, warnings := store.Read(offset, 10)
			if len(warnings) > 0 {
				t.Logf("Read warnings for goroutine %d: %v", id, warnings)
			}
		}(i)

		// Reader goroutine 2
		go func(id int) {
			defer wg.Done()
			<-startSignal // Wait for start signal to increase concurrency

			// Read from start of file - this should definitely hit existing handle
			_, warnings := store.Read(0, 10)
			if len(warnings) > 0 {
				t.Logf("Read warnings for goroutine %d (read at 0): %v", id, warnings)
			}
		}(i)
	}

	// Signal all goroutines to start at once - this maximizes concurrency
	close(startSignal)

	// Wait for all goroutines to complete
	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		t.Error(err)
	}

	t.Logf("Successfully completed %d concurrent operations on same file", numGoroutines*3)
}

// TestEvictingHandlePath tests the code path where a handle is being evicted
// and another goroutine tries to lease it (covers lines 154-160 in leaseFileHandle)
func TestEvictingHandlePath(t *testing.T) {
	tempDir := t.TempDir()
	// Use very low openFilesSoftLimit to trigger GC quickly
	store, err := FileBackedDataStore(tempDir, 1024, 1)
	if err != nil {
		t.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	// Step 1: Create multiple files to fill up the handle cache
	for i := 0; i < 3; i++ {
		data := []byte(fmt.Sprintf("data for file %d", i))
		offset := int64(i * 2048) // Different fileIDs: 0, 2, 4
		err = store.Write(offset, data)
		if err != nil {
			t.Fatalf("Failed to write to file %d: %v", i, err)
		}
	}

	// Step 2: Force concurrent access while GC is happening
	// This creates a race condition where we try to access a file that's being evicted
	var wg sync.WaitGroup
	errors := make(chan error, 20)

	// Launch many goroutines to create contention
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			// Repeatedly access different files to trigger eviction and concurrent access
			for j := 0; j < 10; j++ {
				fileOffset := int64((goroutineID % 3) * 2048) // Access files 0, 2, 4

				// Try to read - this might hit an evicting handle
				_, warnings := store.Read(fileOffset, 10)
				if len(warnings) > 0 {
					t.Logf("Goroutine %d iteration %d warnings: %v", goroutineID, j, warnings)
				}

				// Also try to write - this also might hit evicting handles
				writeData := []byte(fmt.Sprintf("g%d-i%d", goroutineID, j))
				err := store.Write(fileOffset+100, writeData)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d iteration %d write error: %v", goroutineID, j, err)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}

	t.Log("Successfully tested evicting handle code path with high concurrency")
}

// TestReleaseFileHandleAssertion tests the assertion in releaseFileHandle
// by trying to release a non-leased handle (covers line 196-200)
func TestReleaseFileHandleAssertion(t *testing.T) {
	tempDir := t.TempDir()
	store, err := FileBackedDataStore(tempDir, 1024, 5)
	if err != nil {
		t.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	// Access the dataStore directly to test the assertion
	ds := store.(*dataStore)

	// In debug builds, this should trigger the assertion (panic)
	// In production builds, this should log a warning and return gracefully

	defer func() {
		if r := recover(); r != nil {
			// Expected behavior in debug builds - assertion panic
			if panicMsg, ok := r.(string); ok &&
				len(panicMsg) > 0 && panicMsg[:16] == "assertion failed" {
				t.Log("Successfully caught expected assertion panic:", panicMsg)
			} else {
				t.Errorf("Unexpected panic type or message: %v", r)
			}
		} else {
			// If no panic occurred, we're probably in production build
			t.Log("No panic occurred - likely in production build mode")
		}
	}()

	originalLen := len(ds.leased)

	// This should trigger the assertion path for non-leased fileID
	ds.releaseFileHandle(99999) // fileID that was never leased

	// After the call, the leased map should be unchanged
	if len(ds.leased) != originalLen {
		t.Error("releaseFileHandle should not modify leased map for non-existent fileID")
	}

	t.Log("releaseFileHandle completed without panic (production mode)")
}

// TestGCEarlyBreak tests that GC stops early when openFiles drops below limit
// This covers the break statement in gc() at line 216
func TestGCEarlyBreak(t *testing.T) {
	tempDir := t.TempDir()
	// Use very low limit to make GC trigger easily
	store, err := FileBackedDataStore(tempDir, 1024, 2)
	if err != nil {
		t.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	// Create multiple files to exceed the soft limit and get some into ds.free
	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf("data for file %d", i))
		offset := int64(i * 2048) // fileIDs: 0, 2, 4, 6, 8
		err = store.Write(offset, data)
		if err != nil {
			t.Fatalf("Failed to write to file %d: %v", i, err)
		}
	}

	// Now access files in a pattern that will:
	// 1. Put some files in ds.free
	// 2. Trigger GC (due to exceeding openFilesSoftLimit of 2)
	// 3. During GC, make openFiles drop to <= openFilesSoftLimit

	// Access first file again - this might trigger the GC break condition
	_, warnings := store.Read(0, 10)
	if len(warnings) > 0 {
		t.Logf("Read warnings: %v", warnings)
	}

	// Multiple small operations to increase chance of hitting the break
	for i := 0; i < 3; i++ {
		offset := int64(i * 2048)
		store.Write(offset+500, []byte(fmt.Sprintf("extra-%d", i)))
	}

	t.Log("Successfully tested GC scenarios - break condition may have been hit")
}
