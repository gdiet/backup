package internal

import (
	"math"
	"testing"

	"go.etcd.io/bbolt"
)

func TestInitializeFreeAreas(t *testing.T) {
	db := testDB(t)

	t.Run("An empty bucket is initialized with the right entry", func(t *testing.T) {
		freeAreas, cleanupBucket := testBucket(t, db)
		defer cleanupBucket()
		if err := db.Update(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(freeAreas)
			return InitializeFreeAreas(bucket)
		}); err != nil {
			t.Fatalf("Initialize free areas failed: %v", err)
		}
		if err := db.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(freeAreas)
			expectSize(t, bucket, 1)
			expectEntry(t, bucket, U64b(0), U64b(math.MaxInt64))
			return nil
		}); err != nil {
			t.Fatalf("Check bucket size failed: %v", err)
		}
	})

	t.Run("A non-empty bucket is not modified by initialize", func(t *testing.T) {
		freeAreas, cleanupBucket := testBucket(t, db)
		defer cleanupBucket()
		if err := db.Update(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(freeAreas)
			putEntry(t, bucket, U64b(20), U64b(100))
			putEntry(t, bucket, U64b(200), U64b(math.MaxInt64))
			return InitializeFreeAreas(bucket)
		}); err != nil {
			t.Fatalf("Initializing non-empty bucket failed: %v", err)
		}
		if err := db.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(freeAreas)
			expectSize(t, bucket, 2)
			expectEntry(t, bucket, U64b(20), U64b(100))
			expectEntry(t, bucket, U64b(200), U64b(math.MaxInt64))
			return nil
		}); err != nil {
			t.Fatalf("Check bucket size failed: %v", err)
		}
	})
}
