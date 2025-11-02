package cache

import "io"

// Test utility functions

func newEmptyCache() Cache {
	return NewCache("", &EmptyBaseFile{})
}

func newCacheWithBaseData(data bytes) Cache {
	return NewCache("", &data)
}

// Read method of BaseFile implementation using bytes slice.
// This allows testing with predefined data.
func (b *bytes) Read(off int64, data bytes) error {
	if off < 0 || off >= int64(len(*b)) {
		return io.EOF
	}
	copy(data, (*b)[off:])
	return nil
}

// Length method of BaseFile implementation using bytes slice.
// This allows testing with predefined data.
func (b *bytes) Length() int64 {
	return int64(len(*b))
}
