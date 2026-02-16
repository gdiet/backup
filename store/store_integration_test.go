package store_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	dataStore "github.com/gdiet/backup/store"
)

// createDataStore wraps FileBackedDataStore and fails the test if creation fails.
func createDataStore(t *testing.T, dir string, fileSize int64, openFilesSoftLimit int) dataStore.DataStore {
	store, err := dataStore.FileBackedDataStore(dir, fileSize, openFilesSoftLimit)
	if err != nil {
		t.Fatal("Failed to create store:", err)
	}
	return store
}

// TestBasicOperations tests basic read/write operations
func TestBasicOperations(t *testing.T) {
	tempDir := t.TempDir()
	store := createDataStore(t, tempDir, 1024, 5)
	defer store.Close()

	// Test write
	data := []byte("Hello, Storage5!")
	err := store.Write(0, data)
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

// TestErrorHandling tests various error conditions
func TestErrorHandling(t *testing.T) {
	// Test invalid file size
	tempDir := t.TempDir()
	_, err := dataStore.FileBackedDataStore(tempDir, 0, 5)
	if err == nil {
		t.Error("Expected error for zero file size")
	}

	_, err = dataStore.FileBackedDataStore(tempDir, -1, 5)
	if err == nil {
		t.Error("Expected error for negative file size")
	}

	// Test invalid directory
	invalidDir := "/root/non-existent-directory-that-cannot-be-created"
	_, err = dataStore.FileBackedDataStore(invalidDir, 1024, 5)
	if err == nil {
		t.Error("Expected error for invalid directory")
	}

	// Test write with invalid parameters
	store := createDataStore(t, tempDir, 1024, 5)
	defer store.Close()

	// Test write with negative offset
	err = store.Write(-1, []byte("test"))
	if err == nil {
		t.Error("Expected error for negative offset")
	}
}

// TestCrossFileBoundary tests writing and reading across file boundaries
func TestCrossFileBoundary(t *testing.T) {
	tempDir := t.TempDir()
	store := createDataStore(t, tempDir, 100, 5) // Small file size to force boundary crossing
	defer store.Close()

	// Create data that spans multiple files
	data := make([]byte, 250) // 2.5 files
	for i := range data {
		data[i] = byte('A' + (i % 26))
	}

	// Write data starting near the end of first file
	offset := int64(80)
	err := store.Write(offset, data)
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
	store := createDataStore(t, tempDir, 1024*1024, 5) // 1MB files
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

		err := store.Write(offset, data[offset:end])
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
	store := createDataStore(t, tempDir, 1024, 10) // Higher limit for concurrent access
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
	store := createDataStore(t, tempDir, 1024, openFilesSoftLimit)
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

// TestReadNonExistentData tests reading from areas that haven't been written
func TestReadNonExistentData(t *testing.T) {
	tempDir := t.TempDir()
	store := createDataStore(t, tempDir, 1024, 5)
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
	store := createDataStore(t, tempDir, 1024, 5)
	defer store.Close()

	// Write empty data - should not error
	err := store.Write(0, []byte{})
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
	store1 := createDataStore(t, tempDir, 1024, 5)
	data := []byte("Persistent data test")
	err := store1.Write(100, data)
	if err != nil {
		t.Fatal("Write failed:", err)
	}
	store1.Close()

	// Second session: read data
	store2 := createDataStore(t, tempDir, 1024, 5)
	defer store2.Close()

	readData, warnings := store2.Read(100, int64(len(data)))
	if len(warnings) > 0 {
		t.Errorf("Unexpected warnings after reopen: %v", warnings)
	}
	if !bytes.Equal(data, readData) {
		t.Errorf("Data mismatch after reopen. Expected %s, got %s", data, readData)
	}
}

// TestConcurrentSameFile tests concurrent access to the same file to cover
// the "handle already leased" code path in leaseFileHandle.
func TestConcurrentSameFile(t *testing.T) {
	tempDir := t.TempDir()
	store := createDataStore(t, tempDir, 1024, 2) // Low limit to force handle reuse
	defer store.Close()

	// First, create the file by writing some data
	initialData := []byte("initial data to create file")
	err := store.Write(0, initialData)
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
// and another goroutine tries to lease it.
func TestEvictingHandlePath(t *testing.T) {
	tempDir := t.TempDir()
	// Use very low openFilesSoftLimit to trigger GC quickly
	store := createDataStore(t, tempDir, 1024, 1)
	defer store.Close()

	// Step 1: Create multiple files to fill up the handle cache
	for i := 0; i < 3; i++ {
		data := []byte(fmt.Sprintf("data for file %d", i))
		offset := int64(i * 2048) // Different fileIDs: 0, 2, 4
		err := store.Write(offset, data)
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

// TestWriteFileLeaseError tests the error path when leaseFile fails.
// This covers the error return in Write().
func TestWriteFileLeaseError(t *testing.T) {
	tempDir := t.TempDir()
	store := createDataStore(t, tempDir, 100, 5)
	defer store.Close()

	// Calculate the path where the data file would be created
	// For fileID=0: fileName="0000000000", dirNames="000000"
	// dirPath = baseDir/00/00/, filePath = baseDir/00/00/0000000000

	// Create the directory structure manually
	conflictDirPath := filepath.Join(tempDir, "00", "00")
	err := os.MkdirAll(conflictDirPath, 0755)
	if err != nil {
		t.Fatal("Failed to create conflict directory path:", err)
	}

	// Create a directory with the same name as the expected file
	conflictFilePath := filepath.Join(conflictDirPath, "0000000000")
	err = os.Mkdir(conflictFilePath, 0755) // Create DIRECTORY, not file
	if err != nil {
		t.Fatal("Failed to create conflicting directory:", err)
	}

	// Now try to write - this should fail when trying to lease the file
	// because os.OpenFile will try to open "0000000000" as a file, but it's a directory
	data := []byte("This write should fail")
	err = store.Write(0, data) // offset=0 → fileID=0 → conflicts with our directory

	if err == nil {
		t.Fatal("Expected write to fail due to directory conflict, but it succeeded")
	}

	// Check that the error message contains the expected text
	if !strings.Contains(err.Error(), "Unable to lease data file") {
		t.Errorf("Expected error about unable to lease file, got: %v", err)
	}

	// It is also possible that directory creation fails
	// Calculate the path where the data file would be created
	// For fileID=10000: dirPath = baseDir/01/00/

	// Create the directory structure manually
	conflictDirPath = filepath.Join(tempDir, "01")
	err = os.MkdirAll(conflictDirPath, 0755)
	if err != nil {
		t.Fatal("Failed to create conflict directory path:", err)
	}

	// Create a file with the same name as the expected directory
	conflictFilePath = filepath.Join(conflictDirPath, "00")
	err = os.WriteFile(conflictFilePath, []byte("conflicting file"), 0644) // Create FILE, not directory
	if err != nil {
		t.Fatal("Failed to create conflicting file:", err)
	}

	// Now try to write - this should fail when trying to lease the file
	// because the directory can not be created.
	data = []byte("This write should fail")
	err = store.Write(1000000, data) // offset=1000000 → fileID=10000 → conflicts with our directory

	if err == nil {
		t.Fatal("Expected write to fail due to directory conflict, but it succeeded")
	}

	// Check that the error message contains the expected text
	if !strings.Contains(err.Error(), "Unable to lease data file") {
		t.Errorf("Expected error about unable to lease file, got: %v", err)
	}

	t.Log("Successfully tested write failure due to file/directory conflict:", err)
}

// TestExtremelyLargeOffsets tests writing and reading with offsets near MaxInt64
// This verifies that the storage can handle extremely large offsets without overflow
// and that data spans correctly across multiple data files near the boundary
func TestExtremelyLargeOffsets(t *testing.T) {
	tempDir := t.TempDir()

	// Use larger fileSize to make the test more realistic
	fileSize := int64(100 * 1024 * 1024) // 100MB files like production
	store := createDataStore(t, tempDir, fileSize, 5)
	defer store.Close()

	// Calculate an offset that's close to MaxInt64 but leaves room for our test data
	// We want to write across a file boundary, so we pick an offset that will span two files

	// Let's use an offset that puts us near the end of a very high-numbered file
	// but not so close to MaxInt64 that we overflow
	maxSafeFileID := int64(9999999) // Close to the 10M limit mentioned in comments

	// Calculate offset to be near the end of this file, so we span to the next file
	testDataSize := int64(1024)               // 1KB test data
	offsetInFile := fileSize - testDataSize/2 // This will cause span to next file
	baseOffset := maxSafeFileID*fileSize + offsetInFile

	// Ensure we don't exceed MaxInt64
	if baseOffset > 9223372036854775807-testDataSize { // math.MaxInt64 - testDataSize
		t.Skip("Cannot test extremely large offsets on this system - would overflow")
	}

	t.Logf("Testing with baseOffset=%d (file %d), fileSize=%d",
		baseOffset, baseOffset/fileSize, fileSize)

	// Create test data that will span across two files
	testData := make([]byte, testDataSize)
	for i := range testData {
		testData[i] = byte('A' + (i % 26)) // Repeating alphabet pattern
	}

	// Test 1: Write data spanning file boundary
	err := store.Write(baseOffset, testData)
	if err != nil {
		t.Fatalf("Failed to write at extreme offset %d: %v", baseOffset, err)
	}

	t.Logf("Successfully wrote %d bytes at offset %d", len(testData), baseOffset)

	// Test 2: Read back the data
	readData, warnings := store.Read(baseOffset, testDataSize)
	if len(warnings) > 0 {
		t.Logf("Warnings during read at extreme offset: %v", warnings)
	}

	if !bytes.Equal(testData, readData) {
		t.Fatalf("Data mismatch at extreme offset %d", baseOffset)
	}

	t.Logf("Successfully read and verified %d bytes at offset %d", len(readData), baseOffset)

	// Test 3: Verify the data actually spans two different files
	firstFileID := baseOffset / fileSize
	lastByteOffset := baseOffset + testDataSize - 1
	lastFileID := lastByteOffset / fileSize

	if firstFileID == lastFileID {
		t.Errorf("Expected data to span multiple files, but both first (%d) and last (%d) bytes are in file %d",
			baseOffset, lastByteOffset, firstFileID)
	} else {
		t.Logf("Confirmed data spans from file %d to file %d", firstFileID, lastFileID)
	}

	// Test 4: Write and read individual parts to verify both files work independently

	// Part A: Test beginning of the span (in first file)
	partAOffset := baseOffset
	partASize := fileSize - (baseOffset % fileSize) // Bytes remaining in first file
	if partASize > testDataSize {
		partASize = testDataSize
	}

	partAData, warnings := store.Read(partAOffset, partASize)
	if len(warnings) > 0 {
		t.Logf("Warnings reading part A: %v", warnings)
	}

	expectedPartA := testData[:partASize]
	if !bytes.Equal(expectedPartA, partAData) {
		t.Errorf("Part A data mismatch in file %d", firstFileID)
	} else {
		t.Logf("Part A verified: %d bytes in file %d", partASize, firstFileID)
	}

	// Part B: Test continuation in second file (if data actually spans)
	if firstFileID != lastFileID && partASize < testDataSize {
		partBOffset := (firstFileID + 1) * fileSize // Start of next file
		partBSize := testDataSize - partASize

		partBData, warnings := store.Read(partBOffset, partBSize)
		if len(warnings) > 0 {
			t.Logf("Warnings reading part B: %v", warnings)
		}

		expectedPartB := testData[partASize:]
		if !bytes.Equal(expectedPartB, partBData) {
			t.Errorf("Part B data mismatch in file %d", lastFileID)
		} else {
			t.Logf("Part B verified: %d bytes in file %d", partBSize, lastFileID)
		}
	}

	// Test 5: Test edge case - write exactly at file boundary
	exactBoundaryOffset := (maxSafeFileID + 1) * fileSize
	boundaryData := []byte("BOUNDARY_TEST_DATA")

	err = store.Write(exactBoundaryOffset, boundaryData)
	if err != nil {
		t.Fatalf("Failed to write at exact file boundary %d: %v", exactBoundaryOffset, err)
	}

	readBoundaryData, warnings := store.Read(exactBoundaryOffset, int64(len(boundaryData)))
	if len(warnings) > 0 {
		t.Logf("Warnings reading at boundary: %v", warnings)
	}

	if !bytes.Equal(boundaryData, readBoundaryData) {
		t.Fatalf("Boundary data mismatch at offset %d", exactBoundaryOffset)
	}

	t.Logf("Successfully verified write/read at exact file boundary: offset %d (file %d)",
		exactBoundaryOffset, exactBoundaryOffset/fileSize)
}

// TestMaxInt64Offset tests writing at the absolute maximum possible offset
// This ensures the storage can handle the edge case near MaxInt64
func TestMaxInt64Offset(t *testing.T) {
	tempDir := t.TempDir()

	// Use small fileSize for this test to avoid creating huge files
	fileSize := int64(1024) // 1KB files
	store := createDataStore(t, tempDir, fileSize, 5)
	defer store.Close()

	// Calculate the highest safe offset that won't cause integer overflow
	// MaxInt64 = 9223372036854775807
	maxSafeOffset := int64(9223372036854775807 - 100) // Leave 100 bytes buffer

	// Ensure this offset is aligned to avoid partial writes that could overflow
	maxSafeOffset = (maxSafeOffset / fileSize) * fileSize

	t.Logf("Testing at maximum safe offset: %d (file %d)", maxSafeOffset, maxSafeOffset/fileSize)

	// Small test data to avoid overflow
	testData := []byte("MAX_OFFSET_TEST")

	// Test write at maximum offset
	err := store.Write(maxSafeOffset, testData)
	if err != nil {
		t.Fatalf("Failed to write at max offset %d: %v", maxSafeOffset, err)
	}

	// Test read at maximum offset
	readData, warnings := store.Read(maxSafeOffset, int64(len(testData)))
	if len(warnings) > 0 {
		t.Logf("Warnings at max offset: %v", warnings)
	}

	if !bytes.Equal(testData, readData) {
		t.Fatalf("Data mismatch at max offset %d", maxSafeOffset)
	}

	calculatedFileID := maxSafeOffset / fileSize
	t.Logf("Successfully verified write/read at maximum safe offset %d in file %d",
		maxSafeOffset, calculatedFileID)

	// Also test that fileID calculation doesn't overflow
	if calculatedFileID < 0 {
		t.Errorf("FileID calculation overflowed: %d", calculatedFileID)
	}
}
