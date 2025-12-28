package meta

import (
	"backup/src/fserr"
	"bytes"

	"go.etcd.io/bbolt"
)

// lookup resolves a path (array of tree entry names) to both ID and TreeEntry.
// Returns ErrNotFound if any component of the path does not exist.
// An empty path returns the root directory (ID 0 with synthetic root entry).
func lookup(tree *bbolt.Bucket, children *bbolt.Bucket, path []string) (id []byte, entry TreeEntry, err error) {
	// TODO double-check whether we want pointers here and in getChild
	id = make([]byte, 8) // root ID is 0 (as 8 bytes)
	if len(path) == 0 {
		return id, NewDirEntry(""), nil
	}
	for _, component := range path {
		id, entry, err = getChild(tree, children, id, component)
		if err != nil {
			return nil, nil, err
		}
	}
	return id, entry, nil
}

// getChild searches for a child with the given name under the specified parent.
// Returns the child ID as bytes and the tree entry.
// Returns NotFound if the child does not exist.
func getChild(tree *bbolt.Bucket, children *bbolt.Bucket, parentID []byte, name string) ([]byte, TreeEntry, error) {
	cursor := children.Cursor()
	for k, _ := cursor.Seek(parentID); len(k) > 0; k, _ = cursor.Next() {
		if !bytes.HasPrefix(k, parentID) {
			break // No more children for this parent
		}

		if len(k) != 16 {
			return nil, nil, fserr.IO
		}

		childID := k[8:16]
		entry, err := treeEntryFromBytes(tree.Get(childID))
		if err != nil {
			return nil, nil, err
		}

		if entry.Name() == name {
			return childID, entry, nil
		}
	}
	return nil, nil, fserr.NotFound
}
