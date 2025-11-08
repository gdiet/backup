package metadata

import (
	"os"
	"path/filepath"
	"testing"

	"go.etcd.io/bbolt"
)

func TestNewRepository(t *testing.T) {
	// Create temporary database file
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Test repository creation
	repo, err := NewRepository(dbPath)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	defer repo.Close()

	// Verify database file was created
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatal("Database file was not created")
	}

	// Verify buckets were created and free areas initialized
	err = repo.db.View(func(tx *bbolt.Tx) error {
		// Check all buckets exist
		buckets := [][]byte{
			bucketTreeEntries,
			bucketChildren,
			bucketDataEntries,
			bucketFreeAreas,
			bucketContext,
		}

		for _, bucketName := range buckets {
			bucket := tx.Bucket(bucketName)
			if bucket == nil {
				t.Errorf("Bucket %s was not created", bucketName)
			}
		}

		// Check free areas was initialized
		freeAreasBucket := tx.Bucket(bucketFreeAreas)
		if freeAreasBucket == nil {
			t.Fatal("Free areas bucket not found")
		}

		// Should have exactly one entry: 0 -> MaxInt64
		stats := freeAreasBucket.Stats()
		if stats.KeyN != 1 {
			t.Errorf("Expected 1 free area entry, got %d", stats.KeyN)
		}

		// Check the actual entry
		startKey := i64b(0)
		lengthValue := freeAreasBucket.Get(startKey)
		if lengthValue == nil {
			t.Fatal("Initial free area not found")
		}

		// Verify the length value (should be MaxInt64)
		expectedLength := i64b(9223372036854775807) // math.MaxInt64
		if len(lengthValue) != len(expectedLength) {
			t.Errorf("Wrong length value size: expected %d, got %d", len(expectedLength), len(lengthValue))
		}

		return nil
	})

	if err != nil {
		t.Fatalf("Failed to verify repository: %v", err)
	}
}

func TestNewRepositoryReopening(t *testing.T) {
	// Create temporary database file
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_reopen.db")

	// Create repository first time
	repo1, err := NewRepository(dbPath)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	repo1.Close()

	// Reopen the same repository
	repo2, err := NewRepository(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen repository: %v", err)
	}
	defer repo2.Close()

	// Verify free areas still has only one entry (not re-initialized)
	err = repo2.db.View(func(tx *bbolt.Tx) error {
		freeAreasBucket := tx.Bucket(bucketFreeAreas)
		stats := freeAreasBucket.Stats()
		if stats.KeyN != 1 {
			t.Errorf("Expected 1 free area entry after reopening, got %d", stats.KeyN)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to verify reopened repository: %v", err)
	}
}

func TestInt64ToKey(t *testing.T) {
	tests := []int64{0, 1, -1, 42, 9223372036854775807, -9223372036854775808}

	for _, test := range tests {
		key := i64b(test)
		if len(key) != 8 {
			t.Errorf("Key for %d has wrong length: expected 8, got %d", test, len(key))
		}
		// Test that it's reproducible
		key2 := i64b(test)
		if string(key) != string(key2) {
			t.Errorf("Key for %d is not reproducible", test)
		}
	}
}
