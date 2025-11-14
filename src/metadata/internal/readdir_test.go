package internal

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"go.etcd.io/bbolt"
)

func TestReaddirEmptyDirectory(t *testing.T) {
	tempFile, err := os.CreateTemp("", "readdir_empty-*.db")
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

	err = db.Update(func(tx *bbolt.Tx) error {
		tree, err := tx.CreateBucket([]byte("tree_entries"))
		if err != nil {
			return err
		}
		bboltChildren, err := tx.CreateBucket([]byte("children"))
		if err != nil {
			return err
		}
		children := WrapBucket(bboltChildren)

		entries, err := ReaddirForID(tree, children, U64b(0))
		if err != nil {
			t.Errorf("Readdir failed on empty directory: %v", err)
			return err
		}

		if len(entries) != 0 {
			t.Errorf("Expected 0 entries in empty directory, got %d", len(entries))
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}
}

func TestReaddirWithChildren(t *testing.T) {
	tempFile, err := os.CreateTemp("", "readdir_children-*.db")
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

	err = db.Update(func(tx *bbolt.Tx) error {
		tree, err := tx.CreateBucket([]byte("tree_entries"))
		if err != nil {
			return err
		}
		bboltChildren, err := tx.CreateBucket([]byte("children"))
		if err != nil {
			return err
		}
		children := WrapBucket(bboltChildren)

		parent := uint64(0)

		// Create directory child
		childID1 := uint64(10)
		dirEntry := NewDirEntry("subdir1")
		err = tree.Put(U64b(childID1), dirEntry.ToBytes())
		if err != nil {
			return err
		}

		childKey1 := make([]byte, 16)
		copy(childKey1[0:8], U64b(parent))
		copy(childKey1[8:16], U64b(childID1))
		err = children.Put(childKey1, []byte{})
		if err != nil {
			return err
		}

		// Create file child
		childID2 := uint64(20)
		fileEntry := NewFileEntry("file.txt", 1640995200000, [40]byte{1, 2, 3})
		err = tree.Put(U64b(childID2), fileEntry.ToBytes())
		if err != nil {
			return err
		}

		childKey2 := make([]byte, 16)
		copy(childKey2[0:8], U64b(parent))
		copy(childKey2[8:16], U64b(childID2))
		err = children.Put(childKey2, []byte{})
		if err != nil {
			return err
		}

		// Test readdir
		entries, err := ReaddirForID(tree, children, U64b(parent))
		if err != nil {
			t.Errorf("Readdir failed: %v", err)
			return err
		}

		if len(entries) != 2 {
			t.Errorf("Expected 2 entries, got %d", len(entries))
			return nil
		}

		// Verify entries
		names := make(map[string]string)
		for _, entry := range entries {
			switch e := entry.(type) {
			case *DirEntry:
				names[e.Name()] = "dir"
			case *FileEntry:
				names[e.Name()] = "file"
			}
		}

		if names["subdir1"] != "dir" {
			t.Error("Expected 'subdir1' to be a directory")
		}
		if names["file.txt"] != "file" {
			t.Error("Expected 'file.txt' to be a file")
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}
}

func TestReaddirOrphanedChildren(t *testing.T) {
	tempFile, err := os.CreateTemp("", "readdir_orphan-*.db")
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

	defer func() {
		if r := recover(); r != nil {
			// Expected panic due to assertion failure for orphaned tree entry
			panicMsg := fmt.Sprintf("%v", r)
			if !strings.Contains(panicMsg, "assertion failed: invalid tree entry for child ID") {
				t.Errorf("Expected assertion panic about invalid tree entry, got: %v", r)
			}
		} else {
			t.Error("Expected panic due to orphaned tree entry assertion, but no panic occurred")
		}
	}()

	err = db.Update(func(tx *bbolt.Tx) error {
		tree, err := tx.CreateBucket([]byte("tree_entries"))
		if err != nil {
			return err
		}
		bboltChildren, err := tx.CreateBucket([]byte("children"))
		if err != nil {
			return err
		}
		children := WrapBucket(bboltChildren)

		parent := uint64(100)

		// Create valid child
		validChildID := uint64(110)
		dirEntry := NewDirEntry("valid_child")
		err = tree.Put(U64b(validChildID), dirEntry.ToBytes())
		if err != nil {
			return err
		}

		validChildKey := make([]byte, 16)
		copy(validChildKey[0:8], U64b(parent))
		copy(validChildKey[8:16], U64b(validChildID))
		err = children.Put(validChildKey, []byte{})
		if err != nil {
			return err
		}

		// Create orphaned child reference (no tree entry) - this should trigger assertion
		orphanChildID := uint64(999)
		orphanChildKey := make([]byte, 16)
		copy(orphanChildKey[0:8], U64b(parent))
		copy(orphanChildKey[8:16], U64b(orphanChildID))
		err = children.Put(orphanChildKey, []byte{})
		if err != nil {
			return err
		}

		// Test readdir - should encounter assertion for orphaned reference
		entries, err := ReaddirForID(tree, children, U64b(parent))
		if err != nil {
			return err
		}

		// We shouldn't reach here due to expected panic
		t.Logf("Unexpected success: got %d entries", len(entries))
		return nil
	})
	if err != nil {
		t.Logf("Got error instead of expected panic: %v", err)
	}
}

func TestReaddirCorruptedEntry(t *testing.T) {
	tempFile, err := os.CreateTemp("", "readdir_corrupt-*.db")
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

	defer func() {
		if r := recover(); r != nil {
			// Expected panic due to assertion failure for corrupted tree entry
			panicMsg := fmt.Sprintf("%v", r)
			if !strings.Contains(panicMsg, "assertion failed: invalid tree entry for child ID") {
				t.Errorf("Expected assertion panic about invalid tree entry, got: %v", r)
			}
		} else {
			t.Error("Expected panic due to corrupted tree entry assertion, but no panic occurred")
		}
	}()

	err = db.Update(func(tx *bbolt.Tx) error {
		tree, err := tx.CreateBucket([]byte("tree_entries"))
		if err != nil {
			return err
		}
		bboltChildren, err := tx.CreateBucket([]byte("children"))
		if err != nil {
			return err
		}
		children := WrapBucket(bboltChildren)

		parent := uint64(200)

		// Create corrupted tree entry
		corruptedChildID := uint64(210)
		corruptedData := []byte{99} // Invalid tree entry
		err = tree.Put(U64b(corruptedChildID), corruptedData)
		if err != nil {
			return err
		}

		corruptedChildKey := make([]byte, 16)
		copy(corruptedChildKey[0:8], U64b(parent))
		copy(corruptedChildKey[8:16], U64b(corruptedChildID))
		err = children.Put(corruptedChildKey, []byte{})
		if err != nil {
			return err
		}

		// Should encounter assertion for corrupted entry
		_, err = ReaddirForID(tree, children, U64b(parent))
		if err != nil {
			return err
		}

		// We shouldn't reach here due to expected panic
		t.Log("Unexpected success with corrupted entry")
		return nil
	})
	if err != nil {
		t.Logf("Got error instead of expected panic: %v", err)
	}
}

func TestReaddirMultipleParents(t *testing.T) {
	tempFile, err := os.CreateTemp("", "readdir_multi-*.db")
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

	err = db.Update(func(tx *bbolt.Tx) error {
		tree, err := tx.CreateBucket([]byte("tree_entries"))
		if err != nil {
			return err
		}
		bboltChildren, err := tx.CreateBucket([]byte("children"))
		if err != nil {
			return err
		}
		children := WrapBucket(bboltChildren)

		// Create child for parent 0
		parent0 := uint64(0)
		child0ID := uint64(301)
		dirEntry0 := NewDirEntry("parent0_child")
		err = tree.Put(U64b(child0ID), dirEntry0.ToBytes())
		if err != nil {
			return err
		}
		childKey0 := make([]byte, 16)
		copy(childKey0[0:8], U64b(parent0))
		copy(childKey0[8:16], U64b(child0ID))
		err = children.Put(childKey0, []byte{})
		if err != nil {
			return err
		}

		// Create child for parent 1
		parent1 := uint64(1)
		child1ID := uint64(302)
		dirEntry1 := NewDirEntry("parent1_child")
		err = tree.Put(U64b(child1ID), dirEntry1.ToBytes())
		if err != nil {
			return err
		}
		childKey1 := make([]byte, 16)
		copy(childKey1[0:8], U64b(parent1))
		copy(childKey1[8:16], U64b(child1ID))
		err = children.Put(childKey1, []byte{})
		if err != nil {
			return err
		}

		// Test each parent separately
		entries0, err := ReaddirForID(tree, children, U64b(parent0))
		if err != nil {
			t.Errorf("Readdir failed for parent 0: %v", err)
			return err
		}
		if len(entries0) != 1 || entries0[0].Name() != "parent0_child" {
			t.Errorf("Parent 0: expected 1 entry 'parent0_child', got %d entries", len(entries0))
		}

		entries1, err := ReaddirForID(tree, children, U64b(parent1))
		if err != nil {
			t.Errorf("Readdir failed for parent 1: %v", err)
			return err
		}
		if len(entries1) != 1 || entries1[0].Name() != "parent1_child" {
			t.Errorf("Parent 1: expected 1 entry 'parent1_child', got %d entries", len(entries1))
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}
}

func TestReaddirNonExistentParent(t *testing.T) {
	tempFile, err := os.CreateTemp("", "readdir_nonexist-*.db")
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

	err = db.Update(func(tx *bbolt.Tx) error {
		tree, err := tx.CreateBucket([]byte("tree_entries"))
		if err != nil {
			return err
		}
		bboltChildren, err := tx.CreateBucket([]byte("children"))
		if err != nil {
			return err
		}
		children := WrapBucket(bboltChildren)

		// Read from non-existent parent
		entries, err := ReaddirForID(tree, children, U64b(999999))
		if err != nil {
			t.Errorf("Readdir failed for non-existent parent: %v", err)
			return err
		}

		if len(entries) != 0 {
			t.Errorf("Expected 0 entries for non-existent parent, got %d", len(entries))
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}
}
