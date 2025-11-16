package metadata

import (
	"backup/src/metadata/internal"

	"go.etcd.io/bbolt"
)

// Mkdir creates a new directory. It does not check whether the parent exists.
// Returns the ID of the newly created directory.
// Returns ErrExists if a child with the same name already exists under the specified parent.
func (r *Repository) Mkdir(path []string) (uint64, error) {
	if len(path) == 0 {
		return 0, ErrExists // Can't create root directory
	}

	var idBytes []byte
	err := r.db.Update(func(tx *bbolt.Tx) error {
		tree := internal.WrapBucket(tx.Bucket(r.treeKey))
		children := internal.WrapBucket(tx.Bucket(r.childrenKey))

		id, entry, err := internal.Lookup(tree, children, path[:len(path)-1])
		if err != nil {
			return err // ErrNotFound and others
		}

		// Ensure the target is a directory
		if _, isDir := entry.(*DirEntry); !isDir {
			return ErrNotDir
		}

		idBytes, err = internal.Mkdir(tree, children, id, path[len(path)-1])
		return err
	})
	if err != nil {
		return 0, err
	}
	return internal.B64u(idBytes), nil
}
