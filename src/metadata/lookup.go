package metadata

import (
	"backup/src/metadata/internal"

	"go.etcd.io/bbolt"
)

// Lookup looks up a path and returns the ID and tree entry.
// Returns ErrNotFound if the path does not exist.
func (r *Metadata) Lookup(path []string) (id uint64, entry TreeEntry, err error) {
	err = r.db.View(func(tx *bbolt.Tx) error {
		tree := internal.WrapBucket(tx.Bucket(r.treeKey))
		children := internal.WrapBucket(tx.Bucket(r.childrenKey))
		var idBytes []byte
		if idBytes, entry, err = internal.Lookup(tree, children, path); err != nil {
			return err
		}
		id = internal.B64u(idBytes)
		return nil
	})
	return id, entry, err
}
