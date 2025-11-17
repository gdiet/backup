package metadata

import (
	"backup/src/metadata/internal"

	"go.etcd.io/bbolt"
)

// Mkdir creates a new directory.
// Returns the ID of the newly created directory.
// Returns ErrExists if a child with the same name already exists under the specified parent.
// Returns ErrNotFound if the parent directory does not exist.
// Returns ErrNotDir if the parent is not a directory.
func (r *Repository) Mkdir(path []string) (uint64, error) {
	if len(path) == 0 {
		return 0, ErrExists // Can't create root directory
	}

	var idBytes []byte
	err := r.db.Update(func(tx *bbolt.Tx) error {
		tree := internal.WrapBucket(tx.Bucket(r.treeKey))
		children := internal.WrapBucket(tx.Bucket(r.childrenKey))

		parentID, parent, err := internal.Lookup(tree, children, path[:len(path)-1])
		if err != nil {
			return err // ErrNotFound
		}
		// Ensure the parent is a directory
		if _, isDir := parent.(*DirEntry); !isDir {
			return ErrNotDir // Test coverage: needs file implementation
		}

		idBytes, err = internal.Mkdir(tree, children, parentID, path[len(path)-1])
		return err
	})
	if err != nil {
		return 0, err
	}
	return internal.B64u(idBytes), nil
}
