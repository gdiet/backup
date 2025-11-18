package internal

import (
	"os"
	"testing"

	"go.etcd.io/bbolt"
)

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
