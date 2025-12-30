package meta

import (
	"testing"

	"go.etcd.io/bbolt"
)

func TestNextTreeID(t *testing.T) {
	dir := t.TempDir()
	m, _ := NewMetadata(dir)
	m.db.Update(func(tx *bbolt.Tx) error {
		tree := tx.Bucket(m.treeKey)
		id1, err := nextTreeID(tree)
		if err != nil {
			t.Fatalf("Failed to get next tree ID: %v", err)
		}
		if B64u(id1) != 1 {
			t.Fatalf("Expected first call of nextTreeID to return 1, got %d", B64u(id1))
		}
		return nil
	})
}
