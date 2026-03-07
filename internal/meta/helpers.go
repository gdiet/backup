package meta

import "encoding/binary"

// i64w writes an int64 to an existing byte slice.
func i64w(buf []byte, i int64) {
	binary.BigEndian.PutUint64(buf, uint64(i))
}

// b64i converts a byte slice to an int64.
func b64i(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

// u64b converts an uint64 to a byte slice key for bbolt.
func u64b(u uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, u)
	return key
}

// b64u converts a byte slice to a uint64.
func b64u(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// equals checks if two slices of comparable elements are equal.
func equals[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
