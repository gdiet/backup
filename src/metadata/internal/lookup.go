package internal

import (
	"bytes"
	"os"

	"go.etcd.io/bbolt"
)

// Lookup resolves a path (array of tree entry names) to both ID and TreeEntry.
// Returns os.ErrNotExist if any component of the path does not exist.
// An empty path returns the root directory (ID 0 with synthetic root entry).
func Lookup(tree, children *bbolt.Bucket, path []string) (uint64, TreeEntry, error) {
	if len(path) == 0 {
		return 0, NewDirEntry(""), nil
	}

	id := make([]byte, 8) // root id = 0 (as 8 bytes)
	var finalEntry TreeEntry

	for _, component := range path {
		childID, entry, err := getChild(tree, children, id, component)
		if err != nil {
			return 0, nil, err
		}
		id = childID
		finalEntry = entry
	}

	return B64u(id), finalEntry, nil
}

// getChild searches for a child with the given name under the specified parent.
// Returns the child ID as bytes and the tree entry, or os.ErrNotExist if not found.
func getChild(tree, children *bbolt.Bucket, parentID []byte, name string) ([]byte, TreeEntry, error) {
	cursor := children.Cursor()
	for k, _ := cursor.Seek(parentID); len(k) > 0; k, _ = cursor.Next() {
		if !bytes.HasPrefix(k, parentID) {
			break // No more children for this parent
		}

		if len(k) != 16 {
			return nil, nil, &DeserializationError{Msg: "invalid child key length"}
		}

		childID := k[8:16]
		entry, err := treeEntryFromBytes(tree.Get(childID))
		if err != nil {
			return nil, nil, err
		}

		if entry.GetName() == name {
			return childID, entry, nil
		}
	}
	return nil, nil, os.ErrNotExist
}
