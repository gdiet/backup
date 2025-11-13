package internal

import (
	"bytes"
	"os"

	"go.etcd.io/bbolt"
)

// getNextTreeID returns the next available tree entry ID as bytes.
// Start from 1, since 0 is reserved for root.
func getNextTreeID(tree *bbolt.Bucket) []byte {
	var nextID uint64 = 1
	if bytes, _ := tree.Cursor().Last(); bytes != nil {
		nextID = B64u(bytes) + 1
	}
	return U64b(nextID)
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
