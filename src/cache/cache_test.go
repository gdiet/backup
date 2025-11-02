package cache

import (
	"io"
	"os"
	"reflect"
	"testing"
)

const expectedDataMsg = "expected data %v, got %v"

func TestReadEmptyCache(t *testing.T) {
	cache := newEmptyCache()
	data := []byte{1, 2, 3, 4, 5}
	bytesRead, err := cache.Read(0, data)
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if bytesRead != 0 {
		t.Fatalf("expected 0 bytes read, got %d", bytesRead)
	}
	if !reflect.DeepEqual(data, []byte{1, 2, 3, 4, 5}) {
		t.Fatalf("expected data to be unchanged, got %v", data)
	}
}

func TestReadFromBaseFile(t *testing.T) {
	cache := newCacheWithBaseData([]byte{10, 20, 30, 40, 50})
	cache.Write(3, []byte{1}, true, 1000)
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
	data := []byte{}
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
	if err != nil {
		t.Fatalf("expected no error for negative truncate, got %v", err)
	}
	if memoryDelta != 0 {
		t.Fatalf("expected 0 memory delta for negative truncate, got %d", memoryDelta)
	}
	if cache.size != 0 {
		t.Fatalf("expected cache size to remain 0 after negative truncate, got %d", cache.size)
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
	cache.Write(0, []byte{1, 2, 3}, true, 1000)       // memory
	cache.Write(5, []byte{9, 8, 7, 4, 5}, true, 1000) // memory, 5..7 will be overwritten by disk
	cache.Write(5, []byte{5, 4, 3}, false, 1000)      // disk
	cache.Write(10, []byte{2, 1, 9}, false, 1000)     // disk, 12 will be overwritten by memory
	cache.Write(12, []byte{6}, true, 1000)            // memory

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
	memoryDelta, err = cache.Write(1, []byte{}, true, 1000)
	if memoryDelta != 0 || err != nil {
		t.Fatalf("expected write of zero bytes to return 0, nil; got %d, %v", memoryDelta, err)
	}

	// FIXME continue...
	cache.Close()
	_ = os.Remove(path)
}
