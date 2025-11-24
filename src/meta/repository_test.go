package meta

import (
	u "backup/src/util_test"
	"strings"
	"testing"
)

func TestRepositoryBasic(t *testing.T) {
	dbFile := u.TempFile(t)
	repo := testRepoForPath(t, dbFile)
	defer func() { repo.Close() }()

	// readdir on empty root directory
	entries, err := repo.Readdir(nil)
	if err != nil {
		t.Fatalf("Failed to read root directory: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected root directory to be empty, got %d entries", len(entries))
	}

	// mkdir successful
	id, err := repo.Mkdir([]string{"test"})
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if id != 1 {
		t.Errorf("Expected directory ID to be 1, got %d", id)
	}

	// reopen repository
	repo.Close()
	repo = testRepoForPath(t, dbFile)

	// readdir on non-empty root directory
	entries, err = repo.Readdir(nil)
	if err != nil {
		t.Fatalf("Failed to read root directory: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("Expected root directory to have 1 entry, got %d", len(entries))
	}
	if entries[0].Name() != "test" {
		t.Errorf("Expected directory name to be 'test', got '%s'", entries[0].Name())
	}
}

func TestNewRepositoryFailures(t *testing.T) {
	t.Run("file path points to directory", func(t *testing.T) {
		// Create a directory instead of a file
		tempDir := t.TempDir()
		repo, err := NewMetadata(tempDir)
		if err == nil {
			repo.Close()
			t.Fatal("Expected error when opening repository with directory path, but got nil")
		}

		// Verify error message contains relevant information
		if !strings.Contains(err.Error(), "failed to open bbolt database") {
			t.Errorf("Expected error about bbolt database, got: %v", err)
		}
	})

	t.Run("bucket creation failure", func(t *testing.T) {
		dbFile := u.TempFile(t)
		_, err := newMetadata(dbFile, []byte(treeKey), []byte(childrenKey), []byte(dataKey), nil)
		if err == nil {
			t.Fatal("Expected error when creating repository with nil bucket keys, but got nil")
		}
	})
}
