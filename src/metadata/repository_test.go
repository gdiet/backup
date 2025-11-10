package metadata

import (
	"backup/src/metadata/internal"
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
			[]byte(bucketTree),
			[]byte(bucketChildren),
			[]byte(bucketData),
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
		startKey := internal.U64b(0)
		lengthValue := freeAreasBucket.Get(startKey)
		if lengthValue == nil {
			t.Fatal("Initial free area not found")
		}

		// Verify the length value (should be MaxInt64)
		expectedLength := internal.U64b(9223372036854775807) // math.MaxInt64
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
		key := internal.U64b(test)
		if len(key) != 8 {
			t.Errorf("Key for %d has wrong length: expected 8, got %d", test, len(key))
		}
		// Test that it's reproducible
		key2 := internal.U64b(test)
		if string(key) != string(key2) {
			t.Errorf("Key for %d is not reproducible", test)
		}

		// Test round-trip conversion
		restored := internal.B64u(key)
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

func TestRepositoryMkdir(t *testing.T) {
	// Create temporary database file
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "mkdir_test.db")

	// Create repository
	repo, err := NewRepository(dbPath)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	defer repo.Close()

	// Test creating a directory
	err = repo.Mkdir(0, "testdir")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Verify directory was created by checking the internal state
	err = repo.db.View(func(tx *bbolt.Tx) error {
		tree := tx.Bucket([]byte(bucketTree))
		children := tx.Bucket([]byte(bucketChildren))

		// Should have exactly one tree entry now
		stats := tree.Stats()
		if stats.KeyN != 1 {
			t.Errorf("Expected 1 tree entry, got %d", stats.KeyN)
		}

		// Should have exactly one parent-child relationship
		childrenStats := children.Stats()
		if childrenStats.KeyN != 1 {
			t.Errorf("Expected 1 parent-child relationship, got %d", childrenStats.KeyN)
		}

		// Get the entry and verify it's correct
		cursor := tree.Cursor()
		key, value := cursor.First()
		if key == nil {
			t.Fatal("No tree entry found")
		}

		entry, err := internal.TreeEntryFromBytes(value)
		if err != nil {
			return err
		}

		if entry.GetName() != "testdir" {
			t.Errorf("Expected directory name 'testdir', got '%s'", entry.GetName())
		}

		// Verify it's a DirEntry
		_, ok := entry.(*internal.DirEntry)
		if !ok {
			t.Error("Expected DirEntry, got different type")
		}

		return nil
	})

	if err != nil {
		t.Fatalf("Failed to verify directory creation: %v", err)
	}

	// Test creating a duplicate directory (should fail)
	err = repo.Mkdir(0, "testdir")
	if err != os.ErrExist {
		t.Errorf("Expected os.ErrExist for duplicate directory, got: %v", err)
	}

	// Test creating another directory with different name (should succeed)
	err = repo.Mkdir(0, "another")
	if err != nil {
		t.Fatalf("Failed to create second directory: %v", err)
	}

	// Verify both directories exist
	err = repo.db.View(func(tx *bbolt.Tx) error {
		tree := tx.Bucket([]byte(bucketTree))

		// Should have exactly two tree entries now
		stats := tree.Stats()
		if stats.KeyN != 2 {
			t.Errorf("Expected 2 tree entries, got %d", stats.KeyN)
		}

		return nil
	})

	if err != nil {
		t.Fatalf("Failed to verify second directory creation: %v", err)
	}

	// Test name conflict with a file (not just directory)
	t.Run("name conflict with file", func(t *testing.T) {
		// Create a file entry manually to test conflict
		err := repo.db.Update(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte(bucketTree))
			children := tx.Bucket([]byte(bucketChildren))

			// Create a file entry
			fileID := uint64(100)
			fileEntry := &internal.FileEntry{
				Time: 1640995200000,
				Dref: [40]byte{1, 2, 3}, // Sample data reference
				Name: "conflictfile",
			}
			err := tree.Put(internal.U64b(fileID), fileEntry.ToBytes())
			if err != nil {
				return err
			}

			// Create parent-child relationship for this file
			childKey := make([]byte, 16)
			copy(childKey[0:8], internal.U64b(0))       // parent 0
			copy(childKey[8:16], internal.U64b(fileID)) // file child
			return children.Put(childKey, []byte{})
		})
		if err != nil {
			t.Fatalf("Failed to setup file for conflict test: %v", err)
		}

		// Now try to create directory with same name - should fail with os.ErrExist
		err = repo.Mkdir(0, "conflictfile")
		if err != os.ErrExist {
			t.Errorf("Expected os.ErrExist for directory-file name conflict, got: %v", err)
		}
	})
}
