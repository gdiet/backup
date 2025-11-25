package meta

import (
	"backup/src/fserr"
	"backup/src/meta/internal"

	"go.etcd.io/bbolt"
)

// Mkfile creates a new empty file, returning the file ID.
//   - Returns Exists if a child with the same name already exists under the specified parent.
//   - Returns NotFound if the parent directory does not exist.
//   - Returns NotDir if the parent is not a directory.
func (r *Metadata) Mkfile(path []string) (uint64, error) {
	if len(path) == 0 {
		return 0, fserr.Exists // Can't create root directory
	}

	var idBytes []byte
	err := r.db.Update(func(tx *bbolt.Tx) error {
		tree := internal.WrapBucket(tx.Bucket(r.treeKey))
		children := internal.WrapBucket(tx.Bucket(r.childrenKey))

		parentID, parent, err := internal.Lookup(tree, children, path[:len(path)-1])
		if err != nil {
			return err // NotFound
		}
		// Ensure the parent is a directory
		if _, isDir := parent.(*DirEntry); !isDir {
			return fserr.NotDir // Test coverage: needs file implementation
		}

		idBytes, err = internal.Mkfile(tree, children, parentID, path[len(path)-1])
		return err
	})
	if err != nil {
		return 0, err
	}
	return internal.B64u(idBytes), nil
}
