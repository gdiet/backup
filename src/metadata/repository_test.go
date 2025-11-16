package metadata

import (
	"backup/src/metadata/internal"
	u "backup/src/util_test"
	"path/filepath"
	"strings"
	"testing"

	"go.etcd.io/bbolt"
)

func TestNewRepository(t *testing.T) {
	dbFile := u.TempFile(t)
	repo := testRepo(t, dbFile)
	defer func() { repo.Close() }()

	entries, err := repo.Readdir(nil)
	if err != nil {
		t.Fatalf("Failed to read root directory: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected root directory to be empty, got %d entries", len(entries))
	}

	id, err := repo.Mkdir(0, "test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if id != 1 {
		t.Errorf("Expected directory ID to be 1, got %d", id)
	}

	repo.Close()
	repo = testRepo(t, dbFile)

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
		repo, err := NewRepository(tempDir)
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
		_, err := newRepository(dbFile, []byte(treeKey), []byte(childrenKey), []byte(dataKey), nil)
		if err == nil {
			t.Fatal("Expected error when creating repository with nil bucket keys, but got nil")
		}
	})
}

// func TestRepositoryMkdir(t *testing.T) {
// 	// Create temporary database file
// 	tempDir := t.TempDir()
// 	dbPath := filepath.Join(tempDir, "mkdir_test.db")

// 	// Create repository
// 	repo, err := NewRepository(dbPath)
// 	if err != nil {
// 		t.Fatalf("Failed to create repository: %v", err)
// 	}
// 	defer repo.Close()

// 	// Test creating a directory
// 	_, err = repo.Mkdir(0, "testdir")
// 	if err != nil {
// 		t.Fatalf("Failed to create directory: %v", err)
// 	}

// 	// Verify directory was created by checking the internal state
// 	err = repo.db.View(func(tx *bbolt.Tx) error {
// 		tree := tx.Bucket([]byte(treeKey))
// 		children := tx.Bucket([]byte(childrenKey))

// 		// Should have exactly one tree entry now
// 		stats := tree.Stats()
// 		if stats.KeyN != 1 {
// 			t.Errorf("Expected 1 tree entry, got %d", stats.KeyN)
// 		}

// 		// Should have exactly one parent-child relationship
// 		childrenStats := children.Stats()
// 		if childrenStats.KeyN != 1 {
// 			t.Errorf("Expected 1 parent-child relationship, got %d", childrenStats.KeyN)
// 		}

// 		// Verify the directory exists and has correct name using Repository API
// 		entries, err := repo.Readdir([]string{}) // Leerer Pfad für Root-Verzeichnis
// 		if err != nil {
// 			return err
// 		}

// 		if len(entries) != 1 {
// 			t.Errorf("Expected 1 directory entry, got %d", len(entries))
// 			return nil
// 		}

// 		if entries[0].Name() != "testdir" {
// 			t.Errorf("Expected directory name 'testdir', got '%s'", entries[0].Name())
// 		}

// 		// Verify it's a DirEntry
// 		_, ok := entries[0].(*internal.DirEntry)
// 		if !ok {
// 			t.Error("Expected DirEntry, got different type")
// 		}

// 		return nil
// 	})

// 	if err != nil {
// 		t.Fatalf("Failed to verify directory creation: %v", err)
// 	}

// 	// Test creating a duplicate directory (should fail)
// 	_, err = repo.Mkdir(0, "testdir")
// 	if err != os.ErrExist {
// 		t.Errorf("Expected os.ErrExist for duplicate directory, got: %v", err)
// 	}

// 	// Test creating another directory with different name (should succeed)
// 	_, err = repo.Mkdir(0, "another")
// 	if err != nil {
// 		t.Fatalf("Failed to create second directory: %v", err)
// 	}

// 	// Verify both directories exist
// 	err = repo.db.View(func(tx *bbolt.Tx) error {
// 		tree := tx.Bucket([]byte(treeKey))

// 		// Should have exactly two tree entries now
// 		stats := tree.Stats()
// 		if stats.KeyN != 2 {
// 			t.Errorf("Expected 2 tree entries, got %d", stats.KeyN)
// 		}

// 		return nil
// 	})

// 	if err != nil {
// 		t.Fatalf("Failed to verify second directory creation: %v", err)
// 	}

// 	// Test name conflict with a file (not just directory)
// 	t.Run("name conflict with file", func(t *testing.T) {
// 		// Create a file entry manually to test conflict
// 		err := repo.db.Update(func(tx *bbolt.Tx) error {
// 			tree := tx.Bucket([]byte(treeKey))
// 			children := tx.Bucket([]byte(childrenKey))

// 			// Create a file entry
// 			fileID := uint64(100)
// 			fileEntry := internal.NewFileEntry("conflictfile", 1640995200000, [40]byte{1, 2, 3})
// 			err := tree.Put(internal.U64b(fileID), fileEntry.ToBytes())
// 			if err != nil {
// 				return err
// 			}

// 			// Create parent-child relationship for this file
// 			childKey := make([]byte, 16)
// 			copy(childKey[0:8], internal.U64b(0))       // parent 0
// 			copy(childKey[8:16], internal.U64b(fileID)) // file child
// 			return children.Put(childKey, []byte{})
// 		})
// 		if err != nil {
// 			t.Fatalf("Failed to setup file for conflict test: %v", err)
// 		}

// 		// Now try to create directory with same name - should fail with os.ErrExist
// 		_, err = repo.Mkdir(0, "conflictfile")
// 		if err != os.ErrExist {
// 			t.Errorf("Expected os.ErrExist for directory-file name conflict, got: %v", err)
// 		}
// 	})
// }

func TestRepositoryReaddir(t *testing.T) {
	repo, cleanup := createTestRepository(t)
	defer cleanup()

	// Erfolgsfall: Root-Verzeichnis ist leer
	entries, err := repo.Readdir([]string{})
	if err != nil {
		t.Fatalf("unexpected error for root: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries in root, got %d", len(entries))
	}

	// Erfolgsfall: Verzeichnis anlegen und auslesen
	_, err = repo.Mkdir(0, "testdir")
	if err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}
	entries, err = repo.Readdir([]string{"testdir"})
	if err != nil {
		t.Fatalf("unexpected error for testdir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries in testdir, got %d", len(entries))
	}

	// Fehlerfall: Nicht existierendes Verzeichnis
	_, err = repo.Readdir([]string{"doesnotexist"})
	if err == nil {
		t.Error("expected error for non-existent directory")
	}

	// Fehlerfall: Pfad ist Datei, nicht Verzeichnis
	// Datei anlegen
	_, err = repo.Mkdir(0, "dirwithfile")
	if err != nil {
		t.Fatalf("failed to create dirwithfile: %v", err)
	}
	// FileEntry direkt in tree-Bucket anlegen
	err = repo.db.Update(func(tx *bbolt.Tx) error {
		tree := tx.Bucket([]byte(treeKey))
		nextID := internal.U64b(12345)
		fileEntry := internal.NewFileEntry("afile", 0, [40]byte{})
		return tree.Put(nextID, fileEntry.ToBytes())
	})
	if err != nil {
		t.Fatalf("failed to create file entry: %v", err)
	}
	// Lookup auf Datei
	_, err = repo.Readdir([]string{"afile"})
	if err == nil {
		t.Error("expected error for file path in Readdir")
	}
}

// Hilfsfunktion für Test-Repository
func createTestRepository(t *testing.T) (*Repository, func()) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test_readdir.db")
	repo, err := NewRepository(dbPath)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	cleanup := func() { repo.Close() }
	return repo, cleanup
}

func TestRepositoryReaddirFileIsNotDir(t *testing.T) {
	repo, cleanup := createTestRepository(t)
	defer cleanup()

	// Lege eine Datei im Root an
	err := repo.db.Update(func(tx *bbolt.Tx) error {
		tree := tx.Bucket([]byte(treeKey))
		fileID := internal.U64b(42)
		fileEntry := internal.NewFileEntry("myfile.txt", 123, [40]byte{})
		return tree.Put(fileID, fileEntry.ToBytes())
	})
	if err != nil {
		t.Fatalf("Failed to create file entry: %v", err)
	}

	// Erzeuge einen Parent-Child-Eintrag für die Datei
	err = repo.db.Update(func(tx *bbolt.Tx) error {
		children := tx.Bucket([]byte(childrenKey))
		childKey := make([]byte, 16)
		copy(childKey[0:8], internal.U64b(0))   // parent 0
		copy(childKey[8:16], internal.U64b(42)) // file child
		return children.Put(childKey, []byte{})
	})
	if err != nil {
		t.Fatalf("Failed to create parent-child relationship: %v", err)
	}

	// Readdir auf die Datei sollte einen Fehler liefern
	_, err = repo.Readdir([]string{"myfile.txt"})
	if err == nil {
		t.Error("Expected error when calling Readdir on a file, got nil")
	}
}
