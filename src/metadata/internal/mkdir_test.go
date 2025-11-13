package internal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
			_, err = Mkdir(tree, children, 0, "testdir")
			return err
			return err
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

			entry, err := treeEntryFromBytes(value)
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
			_, err = Mkdir(tree, children, 0, "testdir")
			return err
		})

		if err != os.ErrExist {
			t.Errorf("Expected os.ErrExist for duplicate name, got: %v", err)
		}
	})

	t.Run("create different directory name", func(t *testing.T) {
		err := db.Update(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))
			_, err = Mkdir(tree, children, 0, "another")
			return err
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

	t.Run("handle corrupted tree entry", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				// Expected panic due to assertion failure
				panicMsg := fmt.Sprintf("%v", r)
				if !strings.Contains(panicMsg, "assertion failed: invalid tree entry for child ID") {
					t.Errorf("Expected assertion panic about invalid tree entry, got: %v", r)
				}
			} else {
				t.Error("Expected panic due to corrupted tree entry assertion, but no panic occurred")
			}
		}()

		err := db.Update(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))

			// Manually insert corrupted tree entry
			corruptedID := uint64(999)
			corruptedData := []byte{99} // Invalid tree entry data
			err := tree.Put(U64b(corruptedID), corruptedData)
			if err != nil {
				return err
			}

			// Create parent-child relationship to this corrupted entry
			childKey := make([]byte, 16)
			copy(childKey[0:8], U64b(0))            // parent 0
			copy(childKey[8:16], U64b(corruptedID)) // corrupted child
			err = children.Put(childKey, []byte{})
			if err != nil {
				return err
			}

			// Now try to create a directory - should encounter the corrupted entry
			_, err = Mkdir(tree, children, 0, "newdir")
			return err
		})

		// If we reach here without panic, that's unexpected
		if err != nil {
			t.Logf("Got error instead of expected panic: %v", err)
		}
	})

	t.Run("handle empty tree bucket", func(t *testing.T) {
		// Create fresh database for this test
		tempDir := t.TempDir()
		dbPath := filepath.Join(tempDir, "empty_test.db")

		emptyDB, err := bbolt.Open(dbPath, 0600, nil)
		if err != nil {
			t.Fatalf("Failed to open empty test database: %v", err)
		}
		defer emptyDB.Close()

		err = emptyDB.Update(func(tx *bbolt.Tx) error {
			tree, err := tx.CreateBucket([]byte("tree_entries"))
			if err != nil {
				return err
			}
			children, err := tx.CreateBucket([]byte("children"))
			if err != nil {
				return err
			}

			// Create directory in completely empty tree (nextID should start at 0)
			_, err = Mkdir(tree, children, 0, "first")
			return err
		})

		if err != nil {
			t.Errorf("Failed to create directory in empty tree: %v", err)
		}

		// Verify the directory was created with ID 0
		err = emptyDB.View(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))

			// Should have exactly one entry
			stats := tree.Stats()
			if stats.KeyN != 1 {
				t.Errorf("Expected 1 tree entry, got %d", stats.KeyN)
			}

			// Verify it got ID 1 (first ID, since 0 is reserved for root)
			value := tree.Get(U64b(1))
			if value == nil {
				t.Error("Expected entry with ID 1 to exist")
				return nil
			}

			entry, err := treeEntryFromBytes(value)
			if err != nil {
				return err
			}

			if entry.GetName() != "first" {
				t.Errorf("Expected name 'first', got '%s'", entry.GetName())
			}

			return nil
		})

		if err != nil {
			t.Errorf("Verification failed: %v", err)
		}
	})

	t.Run("error handling for tree put operation", func(t *testing.T) {
		// Test error handling by trying to use mkdir in a read-only transaction
		err := db.View(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))

			// This should fail because we're in a read-only transaction
			_, err = Mkdir(tree, children, 0, "readonly_test")
			return err
		})

		if err == nil {
			t.Error("Expected error when trying to write in read-only transaction")
		}
	})

	t.Run("mkdir with file name conflict", func(t *testing.T) {
		err := db.Update(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))

			// Create a file entry first
			fileID := uint64(500)
			fileEntry := NewFileEntry("conflictname", 1640995200000, [40]byte{1, 2, 3})
			err := tree.Put(U64b(fileID), fileEntry.ToBytes())
			if err != nil {
				return err
			}

			// Create parent-child relationship for this file
			childKey := make([]byte, 16)
			copy(childKey[0:8], U64b(0))       // parent 0
			copy(childKey[8:16], U64b(fileID)) // file child
			err = children.Put(childKey, []byte{})
			if err != nil {
				return err
			}

			// Now try to create directory with same name - should fail
			_, err = Mkdir(tree, children, 0, "conflictname")
			return err
		})

		if err != os.ErrExist {
			t.Errorf("Expected os.ErrExist for file name conflict, got: %v", err)
		}
	})

	t.Run("mkdir with missing child entry in tree", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				// Expected panic due to assertion failure for missing tree entry
				panicMsg := fmt.Sprintf("%v", r)
				if !strings.Contains(panicMsg, "assertion failed: invalid tree entry for child ID") {
					t.Errorf("Expected assertion panic about invalid tree entry, got: %v", r)
				}
			} else {
				t.Error("Expected panic due to missing tree entry assertion, but no panic occurred")
			}
		}()

		err := db.Update(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))

			// Create orphaned child relationship (child ID doesn't exist in tree)
			orphanID := uint64(777)
			childKey := make([]byte, 16)
			copy(childKey[0:8], U64b(0))         // parent 0
			copy(childKey[8:16], U64b(orphanID)) // orphan child
			err := children.Put(childKey, []byte{})
			if err != nil {
				return err
			}

			// Now try to create directory - should encounter assertion for missing tree entry
			_, err = Mkdir(tree, children, 0, "afterorphan")
			return err
		})

		// If we reach here without panic, that's unexpected
		if err != nil {
			t.Logf("Got error instead of expected panic: %v", err)
		}
	})

	t.Run("children put operation error", func(t *testing.T) {
		// Create a fresh database to test children bucket error
		tempDir := t.TempDir()
		dbPath := filepath.Join(tempDir, "children_error_test.db")

		childrenDB, err := bbolt.Open(dbPath, 0600, nil)
		if err != nil {
			t.Fatalf("Failed to open children error test database: %v", err)
		}
		defer childrenDB.Close()

		// Create tree bucket first in update transaction
		err = childrenDB.Update(func(tx *bbolt.Tx) error {
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

		// Now test error by using read-only transaction for children bucket
		err = childrenDB.View(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))

			// This should fail when trying to write to children bucket
			_, err = Mkdir(tree, children, 0, "children_error_test")
			return err
		})

		if err == nil {
			t.Error("Expected error when trying to write to children bucket in read-only transaction")
		}
	})

	t.Run("parent has children but no name conflict", func(t *testing.T) {
		err := db.Update(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))

			// Create a directory under parent 1 first
			parent1 := uint64(1)
			childID := uint64(100)

			// Create the child entry in tree
			childEntry := NewDirEntry("child_of_parent1")
			err := tree.Put(U64b(childID), childEntry.ToBytes())
			if err != nil {
				return err
			}

			// Create parent-child relationship for parent 1
			childKey := make([]byte, 16)
			copy(childKey[0:8], U64b(parent1))  // parent 1
			copy(childKey[8:16], U64b(childID)) // child 100
			err = children.Put(childKey, []byte{})
			if err != nil {
				return err
			}

			// Now create directory under parent 0 with different name
			// This should iterate through parent 1's children but find no conflict
			// and hit the break condition when it moves past parent 0's prefix
			_, err = Mkdir(tree, children, 0, "new_dir_under_parent0")
			return err
		})

		if err != nil {
			t.Errorf("Failed to create directory when parent has children but no conflict: %v", err)
		}

		// Verify the directory was created successfully
		err = db.View(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))

			// Find the newly created entry
			cursor := children.Cursor()
			parentPrefix := U64b(0)
			found := false

			for k, _ := cursor.Seek(parentPrefix); len(k) >= 8; k, _ = cursor.Next() {
				if !bytes.HasPrefix(k, parentPrefix) {
					break
				}

				childID := k[8:16]
				entry_bytes := tree.Get(childID)
				if entry_bytes == nil {
					continue
				}

				entry, err := treeEntryFromBytes(entry_bytes)
				if err != nil {
					return err
				}

				if entry.GetName() == "new_dir_under_parent0" {
					found = true
					break
				}
			}

			if !found {
				t.Error("Expected to find 'new_dir_under_parent0' directory")
			}

			return nil
		})

		if err != nil {
			t.Errorf("Verification failed: %v", err)
		}
	})

}
