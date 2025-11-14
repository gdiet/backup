package internal

import (
	"bytes"
	"fmt"
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

// treeEntry retrieves a TreeEntry by its ID bytes
func treeEntry(tree *bbolt.Bucket, id []byte) (TreeEntry, error) {
	bytes := tree.Get(id)
	if bytes == nil { // TODO check - ENOTFOUND?
		return nil, fmt.Errorf("orphaned tree entry for ID %x", id)
	}
	return treeEntryFromBytes(bytes)
}

// getChild searches for a child with the given name under the specified parent.
// Returns the child ID as bytes and the tree entry, or os.ErrNotExist if not found.
func getChild(tree *bbolt.Bucket, children Bucket, parentID []byte, name string) ([]byte, TreeEntry, error) {
	cursor := children.B().Cursor()
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

		if entry.Name() == name {
			return childID, entry, nil
		}
	}
	return nil, nil, os.ErrNotExist
}

// hasChildren checks if a directory has any children
func hasChildren(children *bbolt.Bucket, id []byte) bool {
	cursor := children.Cursor()
	k, _ := cursor.Seek(id)
	return len(k) > 0 && bytes.HasPrefix(k, id)
}
