package aaa

import (
	"io"
	"os"
	"reflect"
	"testing"
)

type emptyMockBaseFile struct{}

func (b emptyMockBaseFile) read(position int, data bytes) error {
	return nil // Always returns no data
}
func (b emptyMockBaseFile) length() int {
	return 0
}

func TestReadEmptyCache(t *testing.T) {
	cache := NewCache("", emptyMockBaseFile{})
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

func TestReadNoData(t *testing.T) {
	cache := NewCache("", emptyMockBaseFile{})
	data := []byte{}
	bytesRead, err := cache.Read(0, data)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if bytesRead != 0 {
		t.Fatalf("expected 0 bytes read, got %d", bytesRead)
	}
}

func TestReadSparseMemoryAndDisk(t *testing.T) {
	path := "test_cache_with_disk.tmp"
	_ = os.Remove(path)

	// 0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15
	// m  m  m  s  s  d  d  d  m  m  d  d  m  s  s  s
	// 1  2  3  0  0  5  4  3  4  5  2  1  6  0  0  0
	cache := NewCache("", emptyMockBaseFile{})
	memoryDelta, err := cache.Truncate(15)
	if memoryDelta != 0 || err != nil {
		t.Fatalf("expected truncate to return 0, nil; got %d, %v", memoryDelta, err)
	}
	// FIXME continue...
}
