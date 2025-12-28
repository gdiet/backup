package meta

import "encoding/binary"

// I64w writes an int64 to an existing byte slice.
func I64w(buf []byte, i int64) {
	binary.BigEndian.PutUint64(buf, uint64(i))
}

// B64i converts a byte slice to an int64.
func B64i(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

// B64u converts a byte slice to a uint64.
func B64u(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
