package internal

import (
	"bytes"
	"os"
	"sync/atomic"
	"testing"

	"go.etcd.io/bbolt"
)

var counter atomic.Uint64

func nextId() uint64 {
	return counter.Add(1)
}

// testDB sets up a temporary bbolt DB for testing.
// Returns the database instance and a cleanup function to close and remove the database file.
func testDB(t *testing.T) (*bbolt.DB, func()) {
	t.Helper()
	tempFile, err := os.CreateTemp("", "test-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	if err := tempFile.Close(); err != nil {
		t.Fatalf("Failed to close temp file: %v", err)
	}
	db, err := bbolt.Open(tempFile.Name(), 0600, nil)
	if err != nil {
		os.Remove(tempFile.Name())
		t.Fatalf("Failed to open database: %v", err)
	}
	return db, func() {
		db.Close()
		os.Remove(tempFile.Name())
	}
}

// testBucket creates a new bucket for testing purposes.
// Returns the bucket and a cleanup function to delete it.
func testBucket(t *testing.T, db *bbolt.DB) ([]byte, func()) {
	t.Helper()
	id := U64b(nextId())
	err := db.Update(func(tx *bbolt.Tx) error {
		var err error
		_, err = tx.CreateBucket(id)
		return err
	})
	if err != nil {
		t.Fatalf("Failed to create or get bucket: %v", err)
	}
	return id, func() {
		db.Update(func(tx *bbolt.Tx) error {
			return tx.DeleteBucket(id)
		})
	}
}

func putEntry(t *testing.T, b *bbolt.Bucket, key []byte, value []byte) {
	t.Helper()
	if err := b.Put(key, value); err != nil {
		t.Fatalf("Failed to put entry: %v", err)
	}
}

func expectEntry(t *testing.T, b *bbolt.Bucket, key []byte, expected []byte) error {
	t.Helper()
	if !bytes.Equal(b.Get(key), expected) {
		t.Errorf("Value mismatch for key %v: expected %v, got %v", key, expected, b.Get(key))
	}
	return nil
}

// expectSize checks that the bucket has the expected number of entries.
// Note that within write transactions, it does not reflect uncommitted changes.
func expectSize(t *testing.T, b *bbolt.Bucket, expected int) error {
	t.Helper()
	if b.Stats().KeyN != expected {
		t.Errorf("Expected bucket to have %d entries, got %d", expected, b.Stats().KeyN)
	}
	return nil
}
