package internal

import (
	"os"
	"path/filepath"
	"testing"

	"go.etcd.io/bbolt"
)

func TestMkdir(t *testing.T) {
	// Create temporary database file
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}
	defer db.Close()

	// Initialize buckets
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucket([]byte("tree_entries"))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucket([]byte("children"))
		return err
	})
	if err != nil {
		t.Fatalf("Failed to create buckets: %v", err)
	}

	t.Run("create directory successfully", func(t *testing.T) {
		err := db.Update(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))
			return Mkdir(tree, children, 0, "testdir")
		})
		if err != nil {
			t.Fatalf("Failed to create directory: %v", err)
		}

		// Verify directory was created
		err = db.View(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))

			// Should have exactly one entry now
			stats := tree.Stats()
			if stats.KeyN != 1 {
				t.Errorf("Expected 1 tree entry, got %d", stats.KeyN)
			}

			// Get the entry and verify it's a directory
			c := tree.Cursor()
			key, value := c.First()
			if key == nil {
				t.Fatal("No tree entry found")
			}

			entry, err := TreeEntryFromBytes(value)
			if err != nil {
				return err
			}

			if entry.GetName() != "testdir" {
				t.Errorf("Expected name 'testdir', got '%s'", entry.GetName())
			}

			// Verify it's a DirEntry
			_, ok := entry.(*DirEntry)
			if !ok {
				t.Error("Expected DirEntry, got different type")
			}

			return nil
		})
		if err != nil {
			t.Errorf("Verification failed: %v", err)
		}
	})

	t.Run("reject duplicate directory name", func(t *testing.T) {
		err := db.Update(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))
			return Mkdir(tree, children, 0, "testdir")
		})

		if err != os.ErrExist {
			t.Errorf("Expected os.ErrExist for duplicate name, got: %v", err)
		}
	})

	t.Run("create different directory name", func(t *testing.T) {
		err := db.Update(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))
			return Mkdir(tree, children, 0, "another")
		})
		if err != nil {
			t.Fatalf("Failed to create second directory: %v", err)
		}

		// Verify both directories exist
		err = db.View(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))

			// Should have exactly two entries now
			stats := tree.Stats()
			if stats.KeyN != 2 {
				t.Errorf("Expected 2 tree entries, got %d", stats.KeyN)
			}
			return nil
		})
		if err != nil {
			t.Errorf("Verification failed: %v", err)
		}
	})
}
