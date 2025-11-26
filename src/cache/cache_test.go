package cache

import (
	"errors"
	"io"
	"os"
	"reflect"
	"syscall"
	"testing"
)

const expectedDataMsg = "expected data %v, got %v"

func TestReadEmptyCache(t *testing.T) {
	cache := newEmptyCache()
	data := bytes{1, 2, 3, 4, 5}
	bytesRead, err := cache.Read(0, data)
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if bytesRead != 0 {
		t.Fatalf("expected 0 bytes read, got %d", bytesRead)
	}
	if !reflect.DeepEqual(data, bytes{1, 2, 3, 4, 5}) {
		t.Fatalf("expected data to be unchanged, got %v", data)
	}
}

func TestReadFromBaseFile(t *testing.T) {
	cache := newCacheWithBaseData(bytes{10, 20, 30, 40, 50})
	cache.Write(3, bytes{1}, true, 1000)
	cache.Truncate(6)
	data := make([]byte, 5)
	bytesRead, err := cache.Read(1, data)
	if err != nil || bytesRead != 5 {
		t.Fatalf("expected no error and 5 bytes read, got %v, %d", err, bytesRead)
	}
	expectedData := []byte{20, 30, 1, 50, 0}
	if !reflect.DeepEqual(data, expectedData) {
		t.Fatalf(expectedDataMsg, expectedData, data)
	}
}

func TestReadNoData(t *testing.T) {
	cache := newEmptyCache()
	var data []byte
	bytesRead, err := cache.Read(0, data)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if bytesRead != 0 {
		t.Fatalf("expected 0 bytes read, got %d", bytesRead)
	}
}

func TestTruncateNegativeSize(t *testing.T) {
	cache := newEmptyCache()
	memoryDelta, err := cache.Truncate(-1)
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("expected EINVAL for negative truncate, got %v", err)
	}
	if memoryDelta != 0 {
		t.Fatalf("expected 0 memory delta for negative truncate, got %d", memoryDelta)
	}
	if cache.size != 0 {
		t.Fatalf("expected cache size to remain 0 after negative truncate, got %d", cache.size)
	}
}

func TestTruncateSameSize(t *testing.T) {
	// Test truncating to the same size (cache.go line 88: return 0, nil // No size change)
	cache := newCacheWithBaseData(bytes{1, 2, 3, 4, 5})

	// Cache should have size 5 from the base data
	if cache.size != 5 {
		t.Fatalf("expected initial cache size 5, got %d", cache.size)
	}

	// Truncate to the same size (5)
	memoryDelta, err := cache.Truncate(5)

	// Should return no error and no memory delta
	if err != nil {
		t.Fatalf("expected no error for same-size truncate, got %v", err)
	}
	if memoryDelta != 0 {
		t.Fatalf("expected 0 memory delta for same-size truncate, got %d", memoryDelta)
	}

	// Cache size should remain unchanged
	if cache.size != 5 {
		t.Fatalf("expected cache size to remain 5 after same-size truncate, got %d", cache.size)
	}
}

func TestReadSparseMemoryAndDisk(t *testing.T) {
	path := "test_cache_with_disk.tmp"
	_ = os.Remove(path)

	// Initialize cache as follows:
	// 0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15
	// m  m  m  s  s  d  d  d  m  m  d  d  m  s  s  s
	// 1  2  3  0  0  5  4  3  4  5  2  1  6  0  0  0
	cache := NewCache(path, &EmptyBaseFile{})
	memoryDelta, err := cache.Truncate(15)
	if memoryDelta != 0 || err != nil {
		t.Fatalf("expected truncate to return 0, nil; got %d, %v", memoryDelta, err)
	}
	cache.Write(0, bytes{1, 2, 3}, true, 1000)       // memory
	cache.Write(5, bytes{9, 8, 7, 4, 5}, true, 1000) // memory, 5..7 will be overwritten by disk
	cache.Write(5, bytes{5, 4, 3}, false, 1000)      // disk
	cache.Write(10, bytes{2, 1, 9}, false, 1000)     // disk, 12 will be overwritten by memory
	cache.Write(12, bytes{6}, true, 1000)            // memory

	// Read from mixed cache
	data := []byte{9, 9, 9, 9}
	bytesRead, err := cache.Read(2, data)
	if err != nil || bytesRead != 4 {
		t.Fatalf("expected no error and 4 bytes read, got %v, %d", err, bytesRead)
	}
	expectedData := []byte{3, 0, 0, 5}
	if !reflect.DeepEqual(data, expectedData) {
		t.Fatalf(expectedDataMsg, expectedData, data)
	}

	// Truncate shrink mixed cache
	memoryDelta, err = cache.Truncate(9)
	if memoryDelta != -2 || err != nil {
		t.Fatalf("expected truncate to return -2, nil; got %d, %v", memoryDelta, err)
	}

	// Read past EOF after truncate
	data = []byte{9, 9, 9, 9}
	bytesRead, err = cache.Read(7, data)
	if err != io.EOF || bytesRead != 2 {
		t.Fatalf("expected EOF and 2 bytes read, got %v, %d", err, bytesRead)
	}
	expectedData = []byte{3, 4, 9, 9}
	if !reflect.DeepEqual(data, expectedData) {
		t.Fatalf(expectedDataMsg, expectedData, data)
	}

	// Write zero bytes
	memoryDelta, err = cache.Write(1, bytes{}, true, 1000)
	if memoryDelta != 0 || err != nil {
		t.Fatalf("expected write of zero bytes to return 0, nil; got %d, %v", memoryDelta, err)
	}

	cache.Close()
	_ = os.Remove(path)
}

func TestWriteBeyondEOF(t *testing.T) {
	// Test writing beyond EOF to create sparse areas (cache.go line 118: c.sparse.write(c.size, off-c.size))
	cache := newCacheWithBaseData(bytes{1, 2, 3}) // Cache size = 3

	// Verify initial size
	if cache.size != 3 {
		t.Fatalf("expected initial cache size 3, got %d", cache.size)
	}

	// Write at offset 5 (beyond EOF at 3), creating a gap from 3-5
	memoryDelta, err := cache.Write(5, bytes{10, 11}, true, 1000)

	// Should succeed without error
	if err != nil {
		t.Fatalf("expected no error for write beyond EOF, got %v", err)
	}

	// Should return positive memory delta (new data was added)
	if memoryDelta <= 0 {
		t.Fatalf("expected positive memory delta for write beyond EOF, got %d", memoryDelta)
	}

	// Cache size should be updated to new EOF (5 + 2 = 7)
	if cache.size != 7 {
		t.Fatalf("expected cache size 7 after write beyond EOF, got %d", cache.size)
	}

	// Verify the sparse gap was created by reading the gap area (offset 3-4)
	gapData := bytes{99, 99} // Initialize with non-zero values
	bytesRead, err := cache.Read(3, gapData)

	if err != nil {
		t.Fatalf("expected no error reading sparse gap, got %v", err)
	}
	if bytesRead != 2 {
		t.Fatalf("expected to read 2 bytes from sparse gap, got %d", bytesRead)
	}
	// Sparse areas should read as zeros
	if gapData[0] != 0 || gapData[1] != 0 {
		t.Fatalf("expected sparse gap to read as zeros, got %v", gapData)
	}

	// Verify the actual written data at offset 5-6
	writtenData := bytes{99, 99} // Initialize with non-zero values
	bytesRead, err = cache.Read(5, writtenData)

	if err != nil {
		t.Fatalf("expected no error reading written data, got %v", err)
	}
	if bytesRead != 2 {
		t.Fatalf("expected to read 2 bytes of written data, got %d", bytesRead)
	}
	// Should read the actual written values
	if writtenData[0] != 10 || writtenData[1] != 11 {
		t.Fatalf("expected written data [10 11], got %v", writtenData)
	}
}
