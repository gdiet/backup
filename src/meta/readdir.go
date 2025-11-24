package meta

import (
	"backup/src/meta/internal"

	"go.etcd.io/bbolt"
)

// Readdir lists the entries under the specified directory (nil or empty for root).
// Returns ErrNotFound if the directory does not exist.
// Returns ErrNotDir if the path is not a directory.
// Can return other errors.
func (r *Metadata) Readdir(path []string) (entries []TreeEntry, err error) {
	err = r.db.View(func(tx *bbolt.Tx) error {
		tree := internal.WrapBucket(tx.Bucket(r.treeKey))
		children := internal.WrapBucket(tx.Bucket(r.childrenKey))

		id, entry, err := internal.Lookup(tree, children, path)
		if err != nil {
			return err // ErrNotFound and others
		}

		// Ensure the target is a directory
		if _, isDir := entry.(*DirEntry); !isDir {
			return ErrNotDir // Test coverage: needs file implementation
		}

		// Read the directory contents
		entries, err = internal.ReaddirForID(tree, children, id)
		return err
	})
	return entries, err
}
