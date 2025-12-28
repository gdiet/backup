package meta

import "go.etcd.io/bbolt"

type Metadata struct {
	db           *bbolt.DB
	treeKey      []byte
	childrenKey  []byte
	dataKey      []byte
	freeAreasKey []byte
}

// Lookup looks up a path and returns the ID and tree entry.
// Returns ErrNotFound if the path does not exist.
func (r *Metadata) Lookup(path []string) (id uint64, entry TreeEntry, err error) {
	err = r.db.View(func(tx *bbolt.Tx) error {
		tree := tx.Bucket(r.treeKey)
		children := tx.Bucket(r.childrenKey)
		var idBytes []byte
		if idBytes, entry, err = lookup(tree, children, path); err != nil {
			return err
		}
		id = B64u(idBytes)
		return nil
	})
	return id, entry, err
}
