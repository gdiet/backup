package internal

import (
	"encoding/binary"

	"go.etcd.io/bbolt"
)

// U64b converts an uint64 to a byte slice key for bbolt.
func U64b(u uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, u)
	return key
}

// B64u converts a byte slice to an uint64.
func B64u(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// U64w writes an uint64 to an existing byte slice.
func U64w(buf []byte, u uint64) {
	binary.BigEndian.PutUint64(buf, u)
}

// I64w writes an int64 to an existing byte slice.
func I64w(buf []byte, i int64) {
	binary.BigEndian.PutUint64(buf, uint64(i))
}

// B64i converts a byte slice to an int64.
func B64i(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

// Bucket is a minimal interface for bbolt.Bucket to allow mocking in tests.
type Bucket interface {
	Put(key, value []byte) error
	B() *bbolt.Bucket
}

type bucket struct {
	b *bbolt.Bucket
}

func (b *bucket) Put(k, v []byte) error {
	return b.b.Put(k, v)
}

func (b *bucket) B() *bbolt.Bucket {
	return b.b
}

func WrapBucket(b *bbolt.Bucket) Bucket {
	return &bucket{b}
}
