package meta

import (
	"backup/src/meta/internal"

	"go.etcd.io/bbolt"
)

// Rmdir removes a directory.
// Returns ErrNotFound if the path does not exist.
// Returns ErrNotDir if the path is not a directory.
// Returns ErrNotEmpty if the directory is not empty.
// Returns ErrIsRoot if the directory is the root.
func (r *Metadata) Rmdir(path []string) error {
	if len(path) == 0 {
		return ErrIsRoot // Can't remove root directory
	}

	return r.db.Update(func(tx *bbolt.Tx) error {
		tree := internal.WrapBucket(tx.Bucket(r.treeKey))
		children := internal.WrapBucket(tx.Bucket(r.childrenKey))

		parentID, _, err := internal.Lookup(tree, children, path[:len(path)-1])
		if err != nil {
			return err // ErrNotFound
		}
		id, entry, err := internal.GetChild(tree, children, parentID, path[len(path)-1])
		if err != nil {
			return err // ErrNotFound
		}
		if _, isDir := entry.(*DirEntry); !isDir {
			return ErrNotDir // Test coverage: needs file implementation
		}

		return internal.Rmdir(tree, children, parentID, id) //ErrNotEmpty
	})
}
