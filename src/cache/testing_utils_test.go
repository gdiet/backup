package cache

import "io"

// Test utility functions

func newEmptyCache() Cache {
	return NewCache("", &EmptyBaseFile{})
}

func newCacheWithBaseData(data bytes) Cache {
	return NewCache("", &data)
}

var _ BaseFile = (*bytes)(nil)

// Read method of BaseFile implementation using bytes slice.
// This allows testing with predefined data.
func (b *bytes) Read(off int64, data bytes) (int, error) {
	copy(data, (*b)[off:])
	if len(data) > len((*b)[off:]) {
		return len((*b)[off:]), io.EOF
	}
	return len(data), nil
}

// Length method of BaseFile implementation using bytes slice.
// This allows testing with predefined data.
func (b *bytes) Length() int64 {
	return int64(len(*b))
}
