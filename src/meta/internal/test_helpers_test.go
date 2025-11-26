package internal

import (
	u "backup/src/util_test"
	"bytes"
	"os"
	"testing"

	"go.etcd.io/bbolt"
)

type bucketFailsPut struct {
	bucket *bbolt.Bucket
}

func (b *bucketFailsPut) Put([]byte, []byte) error {
	return os.ErrInvalid
}

func (b *bucketFailsPut) Get(key []byte) []byte {
	return b.bucket.Get(key)
}

func (b *bucketFailsPut) B() *bbolt.Bucket {
	return b.bucket
}

// testDB sets up a temporary bbolt DB for testing.
// The containing directory is automatically removed when the test and all its subtests complete.
func testDB(t *testing.T) *bbolt.DB {
	t.Helper()
	db, _ := bbolt.Open(u.TempFile(t), 0600, nil)
	return db
}

// testBucket creates a new bucket for testing purposes.
// Returns the bucket and a cleanup function to delete it.
func testBucket(t *testing.T, db *bbolt.DB) ([]byte, func()) {
	t.Helper()
	id := U64b(u.NextId())
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
