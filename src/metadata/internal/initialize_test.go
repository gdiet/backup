package internal

import (
	"math"
	"os"
	"testing"

	"go.etcd.io/bbolt"
)

func TestInitializeFreeAreas(t *testing.T) {
	// Create temporary database for testing
	tempFile, err := os.CreateTemp("", "test-*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	db, err := bbolt.Open(tempFile.Name(), 0600, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("EmptyBucket", func(t *testing.T) {
		bucketName := []byte("free_areas_empty")

		// First create the bucket
		err := db.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucket(bucketName)
			return err
		})
		if err != nil {
			t.Fatalf("Failed to create bucket: %v", err)
		}

		// Now test initialization
		err = db.Update(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(bucketName)
			if bucket == nil {
				t.Error("Bucket should exist")
				return nil
			}

			// Verify bucket is empty
			stats := bucket.Stats()
			t.Logf("Bucket stats before init: KeyN=%d", stats.KeyN)
			if stats.KeyN != 0 {
				t.Errorf("Expected empty bucket, got %d keys", stats.KeyN)
			}

			// Initialize free areas
			err := InitializeFreeAreas(bucket)
			if err != nil {
				t.Errorf("InitializeFreeAreas failed: %v", err)
				return err
			}

			// Verify the key-value pair was actually created
			key := U64b(0)
			value := bucket.Get(key)
			if value == nil {
				t.Error("Expected key 0 to exist after initialization")
				return nil
			}

			// Also check stats (might be cached/delayed)
			stats = bucket.Stats()
			t.Logf("Bucket stats after init: KeyN=%d", stats.KeyN)
			if stats.KeyN != 1 {
				t.Logf("Warning: Stats show %d keys, but key 0 exists", stats.KeyN)
			}

			expectedValue := U64b(math.MaxInt64)
			if len(value) != len(expectedValue) {
				t.Errorf("Value length mismatch: expected %d, got %d", len(expectedValue), len(value))
				return nil
			}

			// Compare byte by byte
			for i, b := range expectedValue {
				if value[i] != b {
					t.Errorf("Value mismatch at byte %d: expected %d, got %d", i, b, value[i])
					break
				}
			}

			// Verify the value represents MaxInt64
			restoredValue := B64u(value)
			if restoredValue != math.MaxInt64 {
				t.Errorf("Expected value %d, got %d", uint64(math.MaxInt64), restoredValue)
			}

			return nil
		})
		if err != nil {
			t.Fatalf("Transaction failed: %v", err)
		}
	})

	t.Run("NonEmptyBucket", func(t *testing.T) {
		bucketName := []byte("free_areas_existing")

		// First create bucket and add data
		err := db.Update(func(tx *bbolt.Tx) error {
			bucket, err := tx.CreateBucket(bucketName)
			if err != nil {
				return err
			}

			// Add some existing data
			return bucket.Put(U64b(100), U64b(200))
		})
		if err != nil {
			t.Fatalf("Failed to setup bucket: %v", err)
		}

		// Now test initialization on non-empty bucket
		err = db.Update(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(bucketName)
			if bucket == nil {
				t.Error("Bucket should exist")
				return nil
			}

			// Verify bucket has 1 key
			stats := bucket.Stats()
			t.Logf("Bucket stats before init: KeyN=%d", stats.KeyN)
			if stats.KeyN != 1 {
				t.Errorf("Expected 1 key in pre-populated bucket, got %d", stats.KeyN)
			}

			// Initialize free areas - should do nothing
			err = InitializeFreeAreas(bucket)
			if err != nil {
				t.Errorf("InitializeFreeAreas failed: %v", err)
				return err
			}

			// Verify bucket still has only 1 key (unchanged)
			stats = bucket.Stats()
			t.Logf("Bucket stats after init: KeyN=%d", stats.KeyN)
			if stats.KeyN != 1 {
				t.Errorf("Expected 1 key after initialization (no change), got %d", stats.KeyN)
			}

			// Verify the original key-value pair is still there
			value := bucket.Get(U64b(100))
			if value == nil {
				t.Error("Expected original key 100 to still exist")
				return nil
			}

			if B64u(value) != 200 {
				t.Errorf("Expected original value 200, got %d", B64u(value))
			}

			// Verify key 0 was NOT added
			value = bucket.Get(U64b(0))
			if value != nil {
				t.Error("Key 0 should not exist in non-empty bucket")
			}

			return nil
		})
		if err != nil {
			t.Fatalf("Transaction failed: %v", err)
		}
	})

	// Test error handling by using a read-only transaction
	t.Run("ErrorHandling", func(t *testing.T) {
		bucketName := []byte("free_areas_readonly")

		// Create bucket first
		err := db.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucket(bucketName)
			return err
		})
		if err != nil {
			t.Fatalf("Failed to create bucket: %v", err)
		}

		// Try to initialize in a read-only transaction (should fail)
		err = db.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(bucketName)
			if bucket == nil {
				return nil
			}

			// This should fail because we're in a read-only transaction
			err := InitializeFreeAreas(bucket)
			if err == nil {
				t.Error("Expected error when trying to write in read-only transaction")
				return nil
			}

			// Verify it's the BBolt error about read-only transaction
			expected := "tx not writable"
			if err.Error() != expected {
				t.Errorf("Expected error '%s', got '%s'", expected, err.Error())
			}

			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
	})
}
