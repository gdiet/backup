package internal

import (
	"os"
	"testing"

	"go.etcd.io/bbolt"
)

func TestGetChild(t *testing.T) {
	// Create temporary database file
	tmpFile := "/tmp/test_getchild.db"
	defer os.Remove(tmpFile)

	db, err := bbolt.Open(tmpFile, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	// Setup test data
	err = db.Update(func(tx *bbolt.Tx) error {
		// Create buckets
		tree, err := tx.CreateBucket([]byte("tree_entries"))
		if err != nil {
			return err
		}
		children, err := tx.CreateBucket([]byte("children"))
		if err != nil {
			return err
		}

		// Create parent directory (ID 0 - root)
		parentID := uint64(0)
		parentIDBytes := U64b(parentID)

		// Create child directory
		childDirID := uint64(1)
		childDirIDBytes := U64b(childDirID)
		dirEntry := NewDirEntry("testdir")
		err = tree.Put(childDirIDBytes, dirEntry.ToBytes())
		if err != nil {
			return err
		}

		// Create child file
		childFileID := uint64(2)
		childFileIDBytes := U64b(childFileID)
		fileEntry := NewFileEntry("testfile.txt", 1640995200000, [40]byte{1, 2, 3})
		err = tree.Put(childFileIDBytes, fileEntry.ToBytes())
		if err != nil {
			return err
		}

		// Create parent-child relationships
		// Key format: parentID (8 bytes) + childID (8 bytes)
		childKey1 := make([]byte, 16)
		copy(childKey1[0:8], parentIDBytes)
		copy(childKey1[8:16], childDirIDBytes)
		err = children.Put(childKey1, []byte{})
		if err != nil {
			return err
		}

		childKey2 := make([]byte, 16)
		copy(childKey2[0:8], parentIDBytes)
		copy(childKey2[8:16], childFileIDBytes)
		err = children.Put(childKey2, []byte{})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Failed to setup test data: %v", err)
	}

	t.Run("FindExistingDirectory", func(t *testing.T) {
		err = db.View(func(tx *bbolt.Tx) error {
			tree := WrapBucket(tx.Bucket([]byte("tree_entries")))
			children := WrapBucket(tx.Bucket([]byte("children")))
			parentID := make([]byte, 8) // Root ID (0)

			childID, entry, err := GetChild(tree, children, parentID, "testdir")
			if err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}

			if len(childID) != 8 {
				t.Errorf("Expected childID length 8, got %d", len(childID))
			}

			expectedID := uint64(1)
			actualID := B64u(childID)
			if actualID != expectedID {
				t.Errorf("Expected childID %d, got %d", expectedID, actualID)
			}

			if entry == nil {
				t.Error("Expected entry to be non-nil")
			}

			if entry.Name() != "testdir" {
				t.Errorf("Expected entry name 'testdir', got '%s'", entry.Name())
			}

			// Check that it's a directory
			if _, ok := entry.(*DirEntry); !ok {
				t.Error("Expected entry to be a DirEntry")
			}

			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
	})

	t.Run("FindExistingFile", func(t *testing.T) {
		err = db.View(func(tx *bbolt.Tx) error {
			tree := WrapBucket(tx.Bucket([]byte("tree_entries")))
			children := WrapBucket(tx.Bucket([]byte("children")))
			parentID := make([]byte, 8) // Root ID (0)

			childID, entry, err := GetChild(tree, children, parentID, "testfile.txt")
			if err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}

			if len(childID) != 8 {
				t.Errorf("Expected childID length 8, got %d", len(childID))
			}

			expectedID := uint64(2)
			actualID := B64u(childID)
			if actualID != expectedID {
				t.Errorf("Expected childID %d, got %d", expectedID, actualID)
			}

			if entry == nil {
				t.Error("Expected entry to be non-nil")
			}

			if entry.Name() != "testfile.txt" {
				t.Errorf("Expected entry name 'testfile.txt', got '%s'", entry.Name())
			}

			// Check that it's a file
			if fileEntry, ok := entry.(*FileEntry); ok {
				if fileEntry.time != 1640995200000 {
					t.Errorf("Expected file time 1640995200000, got %d", fileEntry.time)
				}
			} else {
				t.Error("Expected entry to be a FileEntry")
			}

			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
	})

	t.Run("ChildNotFound", func(t *testing.T) {
		err = db.View(func(tx *bbolt.Tx) error {
			tree := WrapBucket(tx.Bucket([]byte("tree_entries")))
			children := WrapBucket(tx.Bucket([]byte("children")))
			parentID := make([]byte, 8) // Root ID (0)

			childID, entry, err := GetChild(tree, children, parentID, "nonexistent")
			if err != ErrNotFound {
				t.Errorf("Expected ErrNotFound, got: %v", err)
			}

			if childID != nil {
				t.Errorf("Expected nil childID, got: %v", childID)
			}

			if entry != nil {
				t.Errorf("Expected nil entry, got: %v", entry)
			}

			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
	})

	t.Run("ParentHasNoChildren", func(t *testing.T) {
		err = db.View(func(tx *bbolt.Tx) error {
			tree := WrapBucket(tx.Bucket([]byte("tree_entries")))
			children := WrapBucket(tx.Bucket([]byte("children")))
			parentID := U64b(999) // Non-existent parent

			childID, entry, err := GetChild(tree, children, parentID, "anything")
			if err != ErrNotFound {
				t.Errorf("Expected ErrNotFound, got: %v", err)
			}

			if childID != nil {
				t.Errorf("Expected nil childID, got: %v", childID)
			}

			if entry != nil {
				t.Errorf("Expected nil entry, got: %v", entry)
			}

			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
	})

	t.Run("ChildBelongsToOtherParent", func(t *testing.T) {
		// Setup: Create a child that belongs to parent ID 1, then search under parent ID 2
		err = db.Update(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := WrapBucket(tx.Bucket([]byte("children")))

			// Create a child entry under parent ID 1
			parentID1 := uint64(1)
			childID := uint64(100)
			childIDBytes := U64b(childID)

			// Create the child entry in tree
			childEntry := NewDirEntry("isolated_child")
			err = tree.Put(childIDBytes, childEntry.ToBytes())
			if err != nil {
				return err
			}

			// Create parent-child relationship for parent ID 1
			childKey := make([]byte, 16)
			copy(childKey[0:8], U64b(parentID1))
			copy(childKey[8:16], childIDBytes)
			err = children.Put(childKey, []byte{})
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			t.Fatalf("Failed to setup isolated child: %v", err)
		}

		// Test: Search under parent ID 2 (different parent)
		err = db.View(func(tx *bbolt.Tx) error {
			tree := WrapBucket(tx.Bucket([]byte("tree_entries")))
			children := WrapBucket(tx.Bucket([]byte("children")))
			parentID2 := U64b(2) // Different parent

			childID, entry, err := GetChild(tree, children, parentID2, "isolated_child")
			if err != ErrNotFound {
				t.Errorf("Expected ErrNotFound, got: %v", err)
			}

			if childID != nil {
				t.Errorf("Expected nil childID, got: %v", childID)
			}

			if entry != nil {
				t.Errorf("Expected nil entry, got: %v", entry)
			}

			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
	})
}

func TestGetChildErrorCases(t *testing.T) {
	// Create temporary database file
	tmpFile := "/tmp/test_getchild_errors.db"
	defer os.Remove(tmpFile)

	db, err := bbolt.Open(tmpFile, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	t.Run("InvalidChildKeyLength", func(t *testing.T) {
		err = db.Update(func(tx *bbolt.Tx) error {
			// Create buckets
			tree, err := tx.CreateBucket([]byte("tree_entries"))
			if err != nil {
				return err
			}
			bboltChildren, err := tx.CreateBucket([]byte("children"))
			if err != nil {
				return err
			}
			children := WrapBucket(bboltChildren)

			// Create invalid child key (wrong length)
			parentID := make([]byte, 8)
			invalidKey := make([]byte, 10) // Should be 16 bytes
			copy(invalidKey[0:8], parentID)
			err = children.Put(invalidKey, []byte{})
			if err != nil {
				return err
			}

			// Try to find child
			childID, entry, err := GetChild(WrapBucket(tree), children, parentID, "anything")
			if err == nil {
				t.Error("Expected error for invalid key length, got nil")
			}

			if childID != nil {
				t.Errorf("Expected nil childID, got: %v", childID)
			}

			if entry != nil {
				t.Errorf("Expected nil entry, got: %v", entry)
			}

			// Check error type
			if deserErr, ok := err.(*DeserializationError); ok {
				if deserErr.Msg != "invalid child key length" {
					t.Errorf("Expected 'invalid child key length', got '%s'", deserErr.Msg)
				}
			} else {
				t.Errorf("Expected DeserializationError, got %T", err)
			}

			return nil
		})
		if err != nil {
			t.Fatalf("Update transaction failed: %v", err)
		}
	})

	t.Run("CorruptedTreeEntry", func(t *testing.T) {
		err = db.Update(func(tx *bbolt.Tx) error {
			// Get existing buckets (use different names to avoid conflicts)
			tree, err := tx.CreateBucketIfNotExists([]byte("tree_entries_corrupt"))
			if err != nil {
				return err
			}
			bboltChildren, err := tx.CreateBucketIfNotExists([]byte("children_corrupt"))
			if err != nil {
				return err
			}
			children := WrapBucket(bboltChildren)

			// Create valid child key
			parentID := make([]byte, 8)
			childID := U64b(123)
			childKey := make([]byte, 16)
			copy(childKey[0:8], parentID)
			copy(childKey[8:16], childID)
			err = children.Put(childKey, []byte{})
			if err != nil {
				return err
			}

			// Put corrupted tree entry (valid length but invalid type)
			corruptedData := []byte{99, 0} // Invalid tree entry type (99)
			err = tree.Put(childID, corruptedData)
			if err != nil {
				return err
			}

			// Try to find child
			resultID, entry, err := GetChild(WrapBucket(tree), children, parentID, "anything")
			if err == nil {
				t.Error("Expected error for corrupted tree entry, got nil")
			}

			if resultID != nil {
				t.Errorf("Expected nil childID, got: %v", resultID)
			}

			if entry != nil {
				t.Errorf("Expected nil entry, got: %v", entry)
			}

			// Check error type
			if deserErr, ok := err.(*DeserializationError); ok {
				if deserErr.Msg != "invalid treeEntry type" {
					t.Errorf("Expected 'invalid treeEntry type', got '%s'", deserErr.Msg)
				}
			} else {
				t.Errorf("Expected DeserializationError, got %T: %v", err, err)
			}

			return nil
		})
		if err != nil {
			t.Fatalf("Update transaction failed: %v", err)
		}
	})
}

func TestGetNextTreeID(t *testing.T) {
	// Create temporary database file
	tmpFile := "/tmp/test_getnexttreeid.db"
	defer os.Remove(tmpFile)

	db, err := bbolt.Open(tmpFile, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	t.Run("EmptyBucket", func(t *testing.T) {
		err = db.Update(func(tx *bbolt.Tx) error {
			tree, err := tx.CreateBucket([]byte("tree_entries_empty"))
			if err != nil {
				return err
			}

			nextID, err := getNextTreeID(WrapBucket(tree))
			if err != nil {
				return err
			}

			if len(nextID) != 8 {
				t.Errorf("Expected ID length 8, got %d", len(nextID))
			}

			actualID := B64u(nextID)
			expectedActualID := uint64(1)
			if actualID != expectedActualID {
				t.Errorf("Expected ID %d, got %d", expectedActualID, actualID)
			}

			return nil
		})
		if err != nil {
			t.Fatalf("Update transaction failed: %v", err)
		}
	})

	t.Run("WithExistingEntries", func(t *testing.T) {
		err = db.Update(func(tx *bbolt.Tx) error {
			tree, err := tx.CreateBucket([]byte("tree_entries_existing"))
			if err != nil {
				return err
			}

			// Add some existing entries
			var id []byte
			id, err = getNextTreeID(WrapBucket(tree))
			if err != nil {
				return err
			}
			err = tree.Put(id, []byte("dummy1"))
			if err != nil {
				return err
			}
			id, err = getNextTreeID(WrapBucket(tree))
			if err != nil {
				return err
			}
			err = tree.Put(id, []byte("dummy2"))
			if err != nil {
				return err
			}
			id, err = getNextTreeID(WrapBucket(tree))
			if err != nil {
				return err
			}
			err = tree.Put(id, []byte("dummy3"))
			if err != nil {
				return err
			}

			// Should return 11 (highest + 1)
			nextID, err := getNextTreeID(WrapBucket(tree))
			if err != nil {
				return err
			}
			actualID := B64u(nextID)
			expectedID := uint64(4)

			if actualID != expectedID {
				t.Errorf("Expected next ID %d, got %d", expectedID, actualID)
			}

			return nil
		})
		if err != nil {
			t.Fatalf("Update transaction failed: %v", err)
		}
	})
}

func TestHasChildren(t *testing.T) {
	// Create temporary database file
	tmpFile := "/tmp/test_haschildren.db"
	defer os.Remove(tmpFile)

	db, err := bbolt.Open(tmpFile, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	// Setup test data
	err = db.Update(func(tx *bbolt.Tx) error {
		// Create buckets
		children, err := tx.CreateBucket([]byte("children"))
		if err != nil {
			return err
		}

		// Create parent directories
		parentID1 := U64b(1)
		parentID2 := U64b(2)

		// Create children for parent 1
		child1ID := U64b(10)
		child2ID := U64b(11)

		// Create parent-child relationships for parent 1
		childKey1 := make([]byte, 16)
		copy(childKey1[0:8], parentID1)
		copy(childKey1[8:16], child1ID)
		err = children.Put(childKey1, []byte{})
		if err != nil {
			return err
		}

		childKey2 := make([]byte, 16)
		copy(childKey2[0:8], parentID1)
		copy(childKey2[8:16], child2ID)
		err = children.Put(childKey2, []byte{})
		if err != nil {
			return err
		}

		// Create one child for parent 2
		child3ID := U64b(20)
		childKey3 := make([]byte, 16)
		copy(childKey3[0:8], parentID2)
		copy(childKey3[8:16], child3ID)
		err = children.Put(childKey3, []byte{})
		if err != nil {
			return err
		}

		// Parent 3 has no children (no entries in children bucket)

		return nil
	})
	if err != nil {
		t.Fatalf("Failed to setup test data: %v", err)
	}

	t.Run("ParentWithMultipleChildren", func(t *testing.T) {
		err := db.View(func(tx *bbolt.Tx) error {
			children := tx.Bucket([]byte("children"))
			parentID1 := U64b(1)

			result := hasChildren(children, parentID1)
			if !result {
				t.Error("Expected parent 1 to have children, but hasChildren returned false")
			}

			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
	})

	t.Run("ParentWithSingleChild", func(t *testing.T) {
		err := db.View(func(tx *bbolt.Tx) error {
			children := tx.Bucket([]byte("children"))
			parentID2 := U64b(2)

			result := hasChildren(children, parentID2)
			if !result {
				t.Error("Expected parent 2 to have children, but hasChildren returned false")
			}

			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
	})

	t.Run("ParentWithNoChildren", func(t *testing.T) {
		err := db.View(func(tx *bbolt.Tx) error {
			children := tx.Bucket([]byte("children"))
			parentID3 := U64b(3)

			result := hasChildren(children, parentID3)
			if result {
				t.Error("Expected parent 3 to have no children, but hasChildren returned true")
			}

			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
	})

	t.Run("NonExistentParent", func(t *testing.T) {
		err := db.View(func(tx *bbolt.Tx) error {
			children := tx.Bucket([]byte("children"))
			nonExistentID := U64b(999)

			result := hasChildren(children, nonExistentID)
			if result {
				t.Error("Expected non-existent parent to have no children, but hasChildren returned true")
			}

			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
	})

	t.Run("EmptyChildrenBucket", func(t *testing.T) {
		// Create a fresh database with empty children bucket
		tmpFile2 := "/tmp/test_haschildren_empty.db"
		defer os.Remove(tmpFile2)

		db2, err := bbolt.Open(tmpFile2, 0600, nil)
		if err != nil {
			t.Fatalf("Failed to create empty test database: %v", err)
		}
		defer db2.Close()

		err = db2.Update(func(tx *bbolt.Tx) error {
			// Create empty children bucket
			_, err := tx.CreateBucket([]byte("children"))
			return err
		})
		if err != nil {
			t.Fatalf("Failed to create empty bucket: %v", err)
		}

		err = db2.View(func(tx *bbolt.Tx) error {
			children := tx.Bucket([]byte("children"))
			parentID := U64b(1)

			result := hasChildren(children, parentID)
			if result {
				t.Error("Expected empty bucket to show no children, but hasChildren returned true")
			}

			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
	})

	t.Run("PrefixMatchingEdgeCase", func(t *testing.T) {
		err := db.Update(func(tx *bbolt.Tx) error {
			children := tx.Bucket([]byte("children"))

			// Create a scenario where we test prefix matching:
			// Parent ID 4 (0x0000000000000004)
			// Parent ID 40 (0x0000000000000028) - should not match when looking for ID 4
			parentID4 := U64b(4)
			parentID40 := U64b(40)
			childID := U64b(100)

			// Create child for parent 40
			childKey := make([]byte, 16)
			copy(childKey[0:8], parentID40)
			copy(childKey[8:16], childID)
			err := children.Put(childKey, []byte{})
			if err != nil {
				return err
			}

			// Test that parent 4 has no children (should not match parent 40's children)
			result := hasChildren(children, parentID4)
			if result {
				t.Error("Expected parent 4 to have no children (prefix matching edge case), but hasChildren returned true")
			}

			// Test that parent 40 has children
			result = hasChildren(children, parentID40)
			if !result {
				t.Error("Expected parent 40 to have children, but hasChildren returned false")
			}

			return nil
		})
		if err != nil {
			t.Fatalf("Update transaction failed: %v", err)
		}
	})
}
