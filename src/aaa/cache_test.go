package aaa

import (
	"io"
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
	cache := NewCache(emptyMockBaseFile{})
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
