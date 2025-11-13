package internal

import (
	"bytes"
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
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))
			parentID := make([]byte, 8) // Root ID (0)

			childID, entry, err := getChild(tree, children, parentID, "testdir")
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

			if entry.GetName() != "testdir" {
				t.Errorf("Expected entry name 'testdir', got '%s'", entry.GetName())
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
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))
			parentID := make([]byte, 8) // Root ID (0)

			childID, entry, err := getChild(tree, children, parentID, "testfile.txt")
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

			if entry.GetName() != "testfile.txt" {
				t.Errorf("Expected entry name 'testfile.txt', got '%s'", entry.GetName())
			}

			// Check that it's a file
			if fileEntry, ok := entry.(*FileEntry); ok {
				if fileEntry.Time != 1640995200000 {
					t.Errorf("Expected file time 1640995200000, got %d", fileEntry.Time)
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
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))
			parentID := make([]byte, 8) // Root ID (0)

			childID, entry, err := getChild(tree, children, parentID, "nonexistent")
			if err != os.ErrNotExist {
				t.Errorf("Expected os.ErrNotExist, got: %v", err)
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
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))
			parentID := U64b(999) // Non-existent parent

			childID, entry, err := getChild(tree, children, parentID, "anything")
			if err != os.ErrNotExist {
				t.Errorf("Expected os.ErrNotExist, got: %v", err)
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
			children := tx.Bucket([]byte("children"))

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
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))
			parentID2 := U64b(2) // Different parent

			childID, entry, err := getChild(tree, children, parentID2, "isolated_child")
			if err != os.ErrNotExist {
				t.Errorf("Expected os.ErrNotExist, got: %v", err)
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
			children, err := tx.CreateBucket([]byte("children"))
			if err != nil {
				return err
			}

			// Create invalid child key (wrong length)
			parentID := make([]byte, 8)
			invalidKey := make([]byte, 10) // Should be 16 bytes
			copy(invalidKey[0:8], parentID)
			err = children.Put(invalidKey, []byte{})
			if err != nil {
				return err
			}

			// Try to find child
			childID, entry, err := getChild(tree, children, parentID, "anything")
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
			children, err := tx.CreateBucketIfNotExists([]byte("children_corrupt"))
			if err != nil {
				return err
			}

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
			resultID, entry, err := getChild(tree, children, parentID, "anything")
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

func TestLookup(t *testing.T) {
	// Create temporary database file
	tmpFile := "/tmp/test_lookup.db"
	defer os.Remove(tmpFile)

	db, err := bbolt.Open(tmpFile, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	// Setup complex directory structure
	err = db.Update(func(tx *bbolt.Tx) error {
		tree, err := tx.CreateBucket([]byte("tree_entries"))
		if err != nil {
			return err
		}
		children, err := tx.CreateBucket([]byte("children"))
		if err != nil {
			return err
		}

		// Create directory structure:
		// / (root, ID 0)
		// ├── docs/ (ID 1)
		// │   ├── readme.txt (ID 2)
		// │   └── manual/ (ID 3)
		// │       └── guide.pdf (ID 4)
		// └── src/ (ID 5)
		//     └── main.go (ID 6)

		// Root directory (ID 0) - not stored in tree, synthetic

		// Create docs directory (ID 1)
		docsID := uint64(1)
		docsEntry := NewDirEntry("docs")
		err = tree.Put(U64b(docsID), docsEntry.ToBytes())
		if err != nil {
			return err
		}

		// Create readme.txt file (ID 2)
		readmeID := uint64(2)
		readmeEntry := NewFileEntry("readme.txt", 1640995200000, [40]byte{1, 2, 3})
		err = tree.Put(U64b(readmeID), readmeEntry.ToBytes())
		if err != nil {
			return err
		}

		// Create manual directory (ID 3)
		manualID := uint64(3)
		manualEntry := NewDirEntry("manual")
		err = tree.Put(U64b(manualID), manualEntry.ToBytes())
		if err != nil {
			return err
		}

		// Create guide.pdf file (ID 4)
		guideID := uint64(4)
		guideEntry := NewFileEntry("guide.pdf", 1640995300000, [40]byte{4, 5, 6})
		err = tree.Put(U64b(guideID), guideEntry.ToBytes())
		if err != nil {
			return err
		}

		// Create src directory (ID 5)
		srcID := uint64(5)
		srcEntry := NewDirEntry("src")
		err = tree.Put(U64b(srcID), srcEntry.ToBytes())
		if err != nil {
			return err
		}

		// Create main.go file (ID 6)
		mainID := uint64(6)
		mainEntry := NewFileEntry("main.go", 1640995400000, [40]byte{7, 8, 9})
		err = tree.Put(U64b(mainID), mainEntry.ToBytes())
		if err != nil {
			return err
		}

		// Create parent-child relationships
		// Root (0) -> docs (1)
		childKey1 := make([]byte, 16)
		copy(childKey1[0:8], U64b(0))
		copy(childKey1[8:16], U64b(docsID))
		err = children.Put(childKey1, []byte{})
		if err != nil {
			return err
		}

		// Root (0) -> src (5)
		childKey2 := make([]byte, 16)
		copy(childKey2[0:8], U64b(0))
		copy(childKey2[8:16], U64b(srcID))
		err = children.Put(childKey2, []byte{})
		if err != nil {
			return err
		}

		// docs (1) -> readme.txt (2)
		childKey3 := make([]byte, 16)
		copy(childKey3[0:8], U64b(docsID))
		copy(childKey3[8:16], U64b(readmeID))
		err = children.Put(childKey3, []byte{})
		if err != nil {
			return err
		}

		// docs (1) -> manual (3)
		childKey4 := make([]byte, 16)
		copy(childKey4[0:8], U64b(docsID))
		copy(childKey4[8:16], U64b(manualID))
		err = children.Put(childKey4, []byte{})
		if err != nil {
			return err
		}

		// manual (3) -> guide.pdf (4)
		childKey5 := make([]byte, 16)
		copy(childKey5[0:8], U64b(manualID))
		copy(childKey5[8:16], U64b(guideID))
		err = children.Put(childKey5, []byte{})
		if err != nil {
			return err
		}

		// src (5) -> main.go (6)
		childKey6 := make([]byte, 16)
		copy(childKey6[0:8], U64b(srcID))
		copy(childKey6[8:16], U64b(mainID))
		err = children.Put(childKey6, []byte{})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Failed to setup test data: %v", err)
	}

	t.Run("EmptyPath_ReturnsRoot", func(t *testing.T) {
		err = db.View(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))

			id, entry, err := Lookup(tree, children, []string{})
			if err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}

			if !bytes.Equal(id, U64b(0)) {
				t.Errorf("Expected root ID 0, got %v", id)
			}

			if entry == nil {
				t.Error("Expected non-nil entry")
			}

			if entry.GetName() != "" {
				t.Errorf("Expected empty root name, got '%s'", entry.GetName())
			}

			// Check that it's a directory
			if _, ok := entry.(*DirEntry); !ok {
				t.Error("Expected root to be a DirEntry")
			}

			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
	})

	t.Run("SingleLevel_Directory", func(t *testing.T) {
		err = db.View(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))

			id, entry, err := Lookup(tree, children, []string{"docs"})
			if err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}

			if !bytes.Equal(id, U64b(1)) {
				t.Errorf("Expected ID 1, got %v", id)
			}

			if entry.GetName() != "docs" {
				t.Errorf("Expected name 'docs', got '%s'", entry.GetName())
			}

			if _, ok := entry.(*DirEntry); !ok {
				t.Error("Expected DirEntry")
			}

			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
	})

	t.Run("MultiLevel_Directory", func(t *testing.T) {
		err = db.View(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))

			id, entry, err := Lookup(tree, children, []string{"docs", "manual"})
			if err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}

			if !bytes.Equal(id, U64b(3)) {
				t.Errorf("Expected ID 3, got %v", id)
			}

			if entry.GetName() != "manual" {
				t.Errorf("Expected name 'manual', got '%s'", entry.GetName())
			}

			if _, ok := entry.(*DirEntry); !ok {
				t.Error("Expected DirEntry")
			}

			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
	})

	t.Run("SingleLevel_File", func(t *testing.T) {
		err = db.View(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))

			id, entry, err := Lookup(tree, children, []string{"docs", "readme.txt"})
			if err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}

			if !bytes.Equal(id, U64b(2)) {
				t.Errorf("Expected ID 2, got %v", id)
			}

			if entry.GetName() != "readme.txt" {
				t.Errorf("Expected name 'readme.txt', got '%s'", entry.GetName())
			}

			if fileEntry, ok := entry.(*FileEntry); ok {
				if fileEntry.Time != 1640995200000 {
					t.Errorf("Expected time 1640995200000, got %d", fileEntry.Time)
				}
			} else {
				t.Error("Expected FileEntry")
			}

			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
	})

	t.Run("DeepPath_File", func(t *testing.T) {
		err = db.View(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))

			id, entry, err := Lookup(tree, children, []string{"docs", "manual", "guide.pdf"})
			if err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}

			if !bytes.Equal(id, U64b(4)) {
				t.Errorf("Expected ID 4, got %v", id)
			}

			if entry.GetName() != "guide.pdf" {
				t.Errorf("Expected name 'guide.pdf', got '%s'", entry.GetName())
			}

			if fileEntry, ok := entry.(*FileEntry); ok {
				if fileEntry.Time != 1640995300000 {
					t.Errorf("Expected time 1640995300000, got %d", fileEntry.Time)
				}
			} else {
				t.Error("Expected FileEntry")
			}

			return nil
		})
		if err != nil {
			t.Fatalf("View transaction failed: %v", err)
		}
	})

	t.Run("PathNotFound_FirstLevel", func(t *testing.T) {
		err = db.View(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))

			id, entry, err := Lookup(tree, children, []string{"nonexistent"})
			if err != os.ErrNotExist {
				t.Errorf("Expected os.ErrNotExist, got: %v", err)
			}

			if len(id) != 0 {
				t.Errorf("Expected empty ID for non-existent, got %v", id)
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

	t.Run("PathNotFound_SecondLevel", func(t *testing.T) {
		err = db.View(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))

			id, entry, err := Lookup(tree, children, []string{"docs", "nonexistent"})
			if err != os.ErrNotExist {
				t.Errorf("Expected os.ErrNotExist, got: %v", err)
			}

			if len(id) != 0 {
				t.Errorf("Expected empty ID for non-existent, got %v", id)
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

	t.Run("PathThroughFile_ShouldFail", func(t *testing.T) {
		// Try to traverse through a file (readme.txt) as if it were a directory
		err = db.View(func(tx *bbolt.Tx) error {
			tree := tx.Bucket([]byte("tree_entries"))
			children := tx.Bucket([]byte("children"))

			id, entry, err := Lookup(tree, children, []string{"docs", "readme.txt", "something"})
			if err != os.ErrNotExist {
				t.Errorf("Expected os.ErrNotExist when traversing through file, got: %v", err)
			}

			if len(id) != 0 {
				t.Errorf("Expected empty ID, got %v", id)
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
