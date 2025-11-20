package internal

import (
	"testing"

	"go.etcd.io/bbolt"
)

func TestRmdir(t *testing.T) {
	db := testDB(t)
	defer db.Close()
	tree, treeCleanup := testBucket(t, db)
	defer treeCleanup()
	children, childrenCleanup := testBucket(t, db)
	defer childrenCleanup()

	t.Run("delete directory successfully", func(t *testing.T) {
		err := db.Update(func(tx *bbolt.Tx) error {
			tree := WrapBucket(tx.Bucket(tree))
			children := WrapBucket(tx.Bucket(children))
			root := U64b(0)
			id, err := Mkdir(tree, children, root, "testDir")
			if err != nil {
				return err
			}
			return Rmdir(tree, children, root, id)
		})
		if err != nil {
			t.Fatalf("Failed to remove directory: %v", err)
		}

		// Verify directory was removed
		err = db.View(func(tx *bbolt.Tx) error {
			// Tree should have no entries now
			stats := tx.Bucket(tree).Stats()
			if stats.KeyN != 0 {
				t.Errorf("Expected 0 tree entries, got %d", stats.KeyN)
			}
			// Children should have no entries now
			stats = tx.Bucket(children).Stats()
			if stats.KeyN != 0 {
				t.Errorf("Expected 0 children entries, got %d", stats.KeyN)
			}
			return nil
		})
		if err != nil {
			t.Errorf("Verification failed: %v", err)
		}
	})

	t.Run("fail to delete non-empty directory", func(t *testing.T) {
		err := db.Update(func(tx *bbolt.Tx) error {
			tree := WrapBucket(tx.Bucket(tree))
			children := WrapBucket(tx.Bucket(children))
			root := U64b(0)
			id, err := Mkdir(tree, children, root, "testDir")
			if err != nil {
				return err
			}
			_, err = Mkdir(tree, children, id, "innerDir")
			if err != nil {
				return err
			}
			return Rmdir(tree, children, root, id)
		})
		if err != ErrNotEmpty {
			t.Fatalf("Expected ErrNotEmpty, got: %v", err)
		}
	})

	t.Run("delete from bucket fails", func(t *testing.T) {
		root := U64b(0)
		var id []byte
		var err error
		err = db.Update(func(tx *bbolt.Tx) error {
			tree := WrapBucket(tx.Bucket(tree))
			children := WrapBucket(tx.Bucket(children))
			id, err = Mkdir(tree, children, root, "testDir")
			return err
		})
		if err != nil {
			t.Fatalf("Failed to remove directory: %v", err)
		}

		// Remove in a read-only transaction should fail
		err = db.View(func(tx *bbolt.Tx) error {
			tree := WrapBucket(tx.Bucket(tree))
			children := WrapBucket(tx.Bucket(children))
			return Rmdir(tree, children, root, id)
		})
		if err == nil {
			t.Errorf("Expected error when deleting in read-only transaction, got nil")
		}
	})
}
