package meta

import "encoding/binary"

// i64b converts an int64 to a byte slice.
func i64b(i int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

// i64w writes an int64 to a byte slice.
func i64w(b []byte, i int64) {
	binary.BigEndian.PutUint64(b, uint64(i))
}

// b64i converts a byte slice to an int64.
func b64i(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

// equals checks if two slices of comparable elements are equal.
func equals[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a { // same performance: for i, ai := range a { ...
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
