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
