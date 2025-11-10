package metadata

import (
	"os"
	"path/filepath"
	"strings"
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
			[]byte(bucketTreeEntries),
			[]byte(bucketChildren),
			[]byte(bucketDataEntries),
			[]byte(bucketFreeAreas),
			[]byte(bucketContext),
		}

		for _, bucketName := range buckets {
			bucket := tx.Bucket(bucketName)
			if bucket == nil {
				t.Errorf("Bucket %s was not created", bucketName)
			}
		}

		// Check free areas was initialized
		freeAreasBucket := tx.Bucket([]byte(bucketFreeAreas))
		if freeAreasBucket == nil {
			t.Fatal("Free areas bucket not found")
		}

		// Should have exactly one entry: 0 -> MaxInt64
		stats := freeAreasBucket.Stats()
		if stats.KeyN != 1 {
			t.Errorf("Expected 1 free area entry, got %d", stats.KeyN)
		}

		// Check the actual entry
		startKey := u64b(0)
		lengthValue := freeAreasBucket.Get(startKey)
		if lengthValue == nil {
			t.Fatal("Initial free area not found")
		}

		// Verify the length value (should be MaxInt64)
		expectedLength := u64b(9223372036854775807) // math.MaxInt64
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
		freeAreasBucket := tx.Bucket([]byte(bucketFreeAreas))
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

func TestUint64ToKey(t *testing.T) {
	tests := []uint64{0, 1, 42, 9223372036854775807, 18446744073709551615}

	for _, test := range tests {
		key := u64b(test)
		if len(key) != 8 {
			t.Errorf("Key for %d has wrong length: expected 8, got %d", test, len(key))
		}
		// Test that it's reproducible
		key2 := u64b(test)
		if string(key) != string(key2) {
			t.Errorf("Key for %d is not reproducible", test)
		}

		// Test round-trip conversion
		restored := b64u(key)
		if restored != test {
			t.Errorf("Round-trip failed for %d: got %d", test, restored)
		}
	}
}

func TestNewRepositoryFailures(t *testing.T) {
	t.Run("file path points to directory", func(t *testing.T) {
		// Create a directory instead of a file
		tempDir := t.TempDir()
		dirPath := filepath.Join(tempDir, "should_be_file")
		err := os.Mkdir(dirPath, 0755)
		if err != nil {
			t.Fatalf("Failed to create directory for test: %v", err)
		}

		// Attempt to open repository with directory path - should fail
		repo, err := NewRepository(dirPath)
		if err == nil {
			repo.Close()
			t.Fatal("Expected error when opening repository with directory path, but got nil")
		}

		// Verify error message contains relevant information
		if !strings.Contains(err.Error(), "failed to open bbolt database") {
			t.Errorf("Expected error about bbolt database, got: %v", err)
		}
	})

	t.Run("invalid file path", func(t *testing.T) {
		// Try to open repository in non-existent directory with invalid permissions
		invalidPath := "/root/non_existent/test.db" // Likely no permission to create

		repo, err := NewRepository(invalidPath)
		if err == nil {
			repo.Close()
			t.Fatal("Expected error when opening repository with invalid path, but got nil")
		}

		// Verify error message contains relevant information
		if !strings.Contains(err.Error(), "failed to open bbolt database") {
			t.Errorf("Expected error about bbolt database, got: %v", err)
		}
	})

	t.Run("read-only file system", func(t *testing.T) {
		// This test creates a scenario where the file can't be opened for writing
		tempDir := t.TempDir()
		dbPath := filepath.Join(tempDir, "readonly.db")

		// Create the file first
		file, err := os.Create(dbPath)
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		file.Close()

		// Make the file read-only
		err = os.Chmod(dbPath, 0444) // Read-only
		if err != nil {
			t.Fatalf("Failed to make file read-only: %v", err)
		}

		// Make the directory read-only too (this prevents bbolt from creating lock files)
		err = os.Chmod(tempDir, 0555) // Read and execute only
		if err != nil {
			t.Fatalf("Failed to make directory read-only: %v", err)
		}

		// Restore permissions after test
		defer func() {
			os.Chmod(tempDir, 0755)
			os.Chmod(dbPath, 0644)
		}()

		// Attempt to open repository - should fail due to read-only constraints
		repo, err := NewRepository(dbPath)
		if err == nil {
			repo.Close()
			t.Fatal("Expected error when opening repository on read-only file system, but got nil")
		}

		// Verify error message contains relevant information
		if !strings.Contains(err.Error(), "failed to open bbolt database") {
			t.Errorf("Expected error about bbolt database, got: %v", err)
		}
	})
}

func TestNewRepositoryBucketCreationError(t *testing.T) {
	// This test triggers the error path in CreateBucketIfNotExists failure
	// by using an empty bucket name which causes "bucket name is blank" error

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_bucket_error.db") // Use the new constructor with empty bucket name to trigger error
	// This avoids modifying global constants and eliminates race conditions
	repo, err := NewRepositoryWithBuckets(dbPath,
		"", // Empty tree entries bucket name - triggers error
		bucketChildren,
		bucketDataEntries,
		bucketFreeAreas,
		bucketContext)
	if err == nil {
		repo.Close()
		t.Fatal("Expected error when creating bucket with empty name, but got nil")
	}

	// Verify error message contains bucket creation failure
	if !strings.Contains(err.Error(), "failed to create bucket") {
		t.Errorf("Expected error about bucket creation, got: %v", err)
	}
}
