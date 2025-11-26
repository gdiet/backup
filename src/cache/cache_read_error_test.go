package cache

import (
	"errors"
	"os"
	"testing"
)

// MockErrorBaseFile simulates a BaseFile that returns a non-EOF error during Read
type MockErrorBaseFile struct {
	data          bytes
	errorToReturn error
}

var _ BaseFile = (*MockErrorBaseFile)(nil)

func (m *MockErrorBaseFile) Read(off int64, data bytes) (int, error) {
	// Return the configured error instead of reading
	return 0, m.errorToReturn
}

func (m *MockErrorBaseFile) Length() int64 {
	return int64(len(m.data))
}

func TestCacheReadBaseFileError(t *testing.T) {
	// Create a mock BaseFile that returns a non-EOF error
	expectedError := errors.New("simulated disk read error")
	mockBase := &MockErrorBaseFile{
		data:          bytes{1, 2, 3, 4, 5},
		errorToReturn: expectedError,
	}

	// Create cache with the error-producing base
	cache := NewCache("", mockBase)

	// Try to read from an area that would trigger base.Read()
	// Since there's no data in memory/disk cache, it will fall back to base
	data := bytes{0, 0, 0}
	_, err := cache.Read(0, data)

	// Should return the error from base.Read()
	if err != expectedError {
		t.Errorf("expected error %v, got %v", expectedError, err)
	}
}

func TestCacheReadDiskError(t *testing.T) {
	// mainly here for code coverage
	path := "test_cache_read_disk_error.tmp"
	_ = os.Remove(path)

	cache := NewCache(path, &EmptyBaseFile{})
	cache.Write(0, bytes{1, 2, 3}, false, 1000)

	// Close the disk file to trigger a read error later
	cache.disk.file.Close()

	data := bytes{0, 0, 0}
	_, err := cache.Read(0, data)
	_ = os.Remove(path)

	if err == nil {
		t.Fatalf("expected error due to disk read issue, got nil")
	}
}
