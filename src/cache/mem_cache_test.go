package cache

import (
	"bytes"
	"testing"
	"time"
)

func TestNewMemCache(t *testing.T) {
	config := DefaultMemCacheConfig()
	cache, err := NewMemCache(config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer cache.Close()

	stats := cache.GetStats()
	if stats["numberOfFiles"].(int) != 0 {
		t.Errorf("Expected 0 files in new cache, got %d", stats["numberOfFiles"].(int))
	}
}

func TestMemCache_BasicOperations(t *testing.T) {
	cache, err := NewMemCacheDefault()
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer cache.Close()

	fileId := 1
	testData := []byte("Hello, Memory Cache!")

	// Test Write
	err = cache.Write(fileId, 0, testData)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Test Length
	length, err := cache.Length(fileId)
	if err != nil {
		t.Fatalf("Failed to get length: %v", err)
	}
	if length != int64(len(testData)) {
		t.Errorf("Expected length %d, got %d", len(testData), length)
	}

	// Test Read
	readData, err := cache.Read(fileId, 0, len(testData))
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	if !bytes.Equal(testData, readData) {
		t.Errorf("Expected %q, got %q", testData, readData)
	}

	// Test Flush (should be no-op)
	err = cache.Flush(fileId)
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
}

func TestMemCache_Truncate(t *testing.T) {
	cache, err := NewMemCacheDefault()
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer cache.Close()

	fileId := 1
	testData := []byte("Hello, World!")

	// Write initial data
	err = cache.Write(fileId, 0, testData)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Truncate to smaller size
	err = cache.Truncate(fileId, 5)
	if err != nil {
		t.Fatalf("Failed to truncate: %v", err)
	}

	// Check length
	length, err := cache.Length(fileId)
	if err != nil {
		t.Fatalf("Failed to get length: %v", err)
	}
	if length != 5 {
		t.Errorf("Expected length 5, got %d", length)
	}

	// Check data
	readData, err := cache.Read(fileId, 0, 5)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	expected := testData[:5]
	if !bytes.Equal(expected, readData) {
		t.Errorf("Expected %q, got %q", expected, readData)
	}

	// Truncate to larger size (should extend with zeros)
	err = cache.Truncate(fileId, 10)
	if err != nil {
		t.Fatalf("Failed to truncate larger: %v", err)
	}

	length, err = cache.Length(fileId)
	if err != nil {
		t.Fatalf("Failed to get length: %v", err)
	}
	if length != 10 {
		t.Errorf("Expected length 10, got %d", length)
	}

	// Read extended part (should be zeros)
	readData, err = cache.Read(fileId, 5, 5)
	if err != nil {
		t.Fatalf("Failed to read extended part: %v", err)
	}
	expectedZeros := make([]byte, 5)
	if !bytes.Equal(expectedZeros, readData) {
		t.Errorf("Expected zeros, got %q", readData)
	}
}

func TestMemCache_RandomAccess(t *testing.T) {
	cache, err := NewMemCacheDefault()
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer cache.Close()

	fileId := 1

	// Write at different positions
	err = cache.Write(fileId, 0, []byte("Hello"))
	if err != nil {
		t.Fatalf("Failed to write at position 0: %v", err)
	}

	err = cache.Write(fileId, 10, []byte("World"))
	if err != nil {
		t.Fatalf("Failed to write at position 10: %v", err)
	}

	// Check length (should be 15: 10 + len("World"))
	length, err := cache.Length(fileId)
	if err != nil {
		t.Fatalf("Failed to get length: %v", err)
	}
	if length != 15 {
		t.Errorf("Expected length 15, got %d", length)
	}

	// Read first part
	data1, err := cache.Read(fileId, 0, 5)
	if err != nil {
		t.Fatalf("Failed to read first part: %v", err)
	}
	if !bytes.Equal([]byte("Hello"), data1) {
		t.Errorf("Expected 'Hello', got %q", data1)
	}

	// Read gap (should be zeros)
	data2, err := cache.Read(fileId, 5, 5)
	if err != nil {
		t.Fatalf("Failed to read gap: %v", err)
	}
	expectedZeros := make([]byte, 5)
	if !bytes.Equal(expectedZeros, data2) {
		t.Errorf("Expected zeros, got %q", data2)
	}

	// Read second part
	data3, err := cache.Read(fileId, 10, 5)
	if err != nil {
		t.Fatalf("Failed to read second part: %v", err)
	}
	if !bytes.Equal([]byte("World"), data3) {
		t.Errorf("Expected 'World', got %q", data3)
	}
}

func TestMemCache_Dispose(t *testing.T) {
	cache, err := NewMemCacheDefault()
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer cache.Close()

	fileId := 1
	testData := []byte("Test data")

	// Write data
	err = cache.Write(fileId, 0, testData)
	if err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Verify it exists
	length, err := cache.Length(fileId)
	if err != nil {
		t.Fatalf("Failed to get length: %v", err)
	}
	if length != int64(len(testData)) {
		t.Errorf("Expected length %d, got %d", len(testData), length)
	}

	// Dispose the file
	err = cache.Dispose(fileId)
	if err != nil {
		t.Fatalf("Failed to dispose: %v", err)
	}

	// Try to read (should fail)
	_, err = cache.Length(fileId)
	if err == nil {
		t.Error("Expected error when accessing disposed file")
	}

	// Dispose again (should be idempotent)
	err = cache.Dispose(fileId)
	if err != nil {
		t.Fatalf("Failed to dispose twice: %v", err)
	}
}

func TestMemCache_MultipleFiles(t *testing.T) {
	cache, err := NewMemCacheDefault()
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer cache.Close()

	// Create multiple files with different data
	files := map[int][]byte{
		1: []byte("File 1 data"),
		2: []byte("File 2 has different content"),
		3: []byte("File 3"),
	}

	// Write all files
	for fileId, data := range files {
		err = cache.Write(fileId, 0, data)
		if err != nil {
			t.Fatalf("Failed to write file %d: %v", fileId, err)
		}
	}

	// Verify all files
	for fileId, expectedData := range files {
		readData, err := cache.Read(fileId, 0, len(expectedData))
		if err != nil {
			t.Fatalf("Failed to read file %d: %v", fileId, err)
		}

		if !bytes.Equal(expectedData, readData) {
			t.Errorf("Data mismatch for file %d: expected %q, got %q", fileId, expectedData, readData)
		}
	}

	stats := cache.GetStats()
	if stats["numberOfFiles"].(int) != len(files) {
		t.Errorf("Expected %d files, got %d", len(files), stats["numberOfFiles"].(int))
	}
}

func TestMemCache_ErrorConditions(t *testing.T) {
	cache, err := NewMemCacheDefault()
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer cache.Close()

	fileId := 1

	// Test negative position
	err = cache.Write(fileId, -1, []byte("test"))
	if err == nil {
		t.Error("Expected error for negative position")
	}

	err = cache.Truncate(fileId, -1)
	if err == nil {
		t.Error("Expected error for negative truncate length")
	}

	_, err = cache.Read(fileId, -1, 10)
	if err == nil {
		t.Error("Expected error for negative read position")
	}

	_, err = cache.Read(fileId, 0, -1)
	if err == nil {
		t.Error("Expected error for negative read length")
	}

	// Test reading from non-existent file (should create it)
	data, err := cache.Read(999, 0, 10)
	if err != nil {
		t.Fatalf("Unexpected error reading from new file: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("Expected empty data from new file, got %d bytes", len(data))
	}
}

func TestMemCache_Configuration(t *testing.T) {
	config := MemCacheConfig{
		MinSize:         1024, // 1KB min
		MaxSize:         -1,   // Use percentage
		MaxPercent:      0.5,  // 50%
		UpdateInterval:  time.Millisecond * 100,
		ShrinkThreshold: 0.8,
		GrowThreshold:   0.6,
	}

	cache, err := NewMemCache(config)
	if err != nil {
		t.Fatalf("Failed to create cache with custom config: %v", err)
	}
	defer cache.Close()

	stats := cache.GetStats()
	if stats["configMinSize"].(uint64) != config.MinSize {
		t.Errorf("Expected MinSize %d, got %d", config.MinSize, stats["configMinSize"].(uint64))
	}
	if stats["configMaxSize"].(int64) != config.MaxSize {
		t.Errorf("Expected MaxSize %d, got %d", config.MaxSize, stats["configMaxSize"].(int64))
	}
	if stats["configMaxPercent"].(float64) != config.MaxPercent {
		t.Errorf("Expected MaxPercent %f, got %f", config.MaxPercent, stats["configMaxPercent"].(float64))
	}
}

func TestMemCache_DisabledCache(t *testing.T) {
	config := MemCacheConfig{
		MinSize:    1024, // Will be ignored when MaxSize = 0
		MaxSize:    0,    // Disabled cache
		MaxPercent: 0.5,  // Will be ignored
	}

	cache, err := NewMemCache(config)
	if err != nil {
		t.Fatalf("Failed to create disabled cache: %v", err)
	}
	defer cache.Close()

	// Check that limits are set to 0
	stats := cache.GetStats()
	if stats["currentLimit"].(uint64) != 0 {
		t.Errorf("Expected currentLimit 0 for disabled cache, got %d", stats["currentLimit"].(uint64))
	}

	// All operations should fail
	fileId := 1
	testData := []byte("test")

	err = cache.Write(fileId, 0, testData)
	if err == nil {
		t.Error("Expected error when writing to disabled cache")
	}

	err = cache.Truncate(fileId, 10)
	if err == nil {
		t.Error("Expected error when truncating in disabled cache")
	}

	// Read and Length should work (return empty/error for non-existent files)
	data, err := cache.Read(fileId, 0, 10)
	if err != nil {
		t.Fatalf("Read should work even with disabled cache: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("Expected empty data from disabled cache, got %d bytes", len(data))
	}
}

func TestMemCache_Close(t *testing.T) {
	cache, err := NewMemCacheDefault()
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}

	// Add some data
	for i := 1; i <= 3; i++ {
		err = cache.Write(i, 0, []byte("test data"))
		if err != nil {
			t.Fatalf("Failed to write file %d: %v", i, err)
		}
	}

	stats := cache.GetStats()
	if stats["numberOfFiles"].(int) != 3 {
		t.Errorf("Expected 3 files, got %d", stats["numberOfFiles"].(int))
	}

	// Close the cache
	err = cache.Close()
	if err != nil {
		t.Fatalf("Failed to close cache: %v", err)
	}

	// Check that everything is cleaned up
	stats = cache.GetStats()
	if stats["numberOfFiles"].(int) != 0 {
		t.Errorf("Expected 0 files after close, got %d", stats["numberOfFiles"].(int))
	}
	if stats["currentSize"].(uint64) != 0 {
		t.Errorf("Expected 0 size after close, got %d", stats["currentSize"].(uint64))
	}
}

// Benchmark tests
func BenchmarkMemCache_Write(b *testing.B) {
	cache, err := NewMemCacheDefault()
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	data := make([]byte, 1024) // 1KB
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = cache.Write(1, int64(i*1024), data)
		if err != nil {
			b.Fatalf("Failed to write: %v", err)
		}
	}
}

func BenchmarkMemCache_Read(b *testing.B) {
	cache, err := NewMemCacheDefault()
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Prepare data
	data := make([]byte, 1024*1024) // 1MB
	for i := range data {
		data[i] = byte(i % 256)
	}
	err = cache.Write(1, 0, data)
	if err != nil {
		b.Fatalf("Failed to write test data: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = cache.Read(1, int64(i%1000)*1024, 1024)
		if err != nil {
			b.Fatalf("Failed to read: %v", err)
		}
	}
}
