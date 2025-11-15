package internal

import (
	"bytes"

	"go.etcd.io/bbolt"
)

// getNextTreeID returns the next available tree entry ID as bytes.
// Start from 1, since 0 is reserved for root.
func getNextTreeID(tree *bbolt.Bucket) ([]byte, error) {
	id, err := tree.NextSequence()
	return U64b(id), err
}

// treeEntry retrieves a TreeEntry by its ID bytes
// Returns ErrNotFound if the entry does not exist.
func treeEntry(tree *bbolt.Bucket, id []byte) (TreeEntry, error) {
	bytes := tree.Get(id)
	if bytes == nil {
		return nil, ErrNotFound
	}
	return treeEntryFromBytes(bytes)
}

// getChild searches for a child with the given name under the specified parent.
// Returns the child ID as bytes and the tree entry.
// Returns ErrNotFound if the child does not exist.
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
	return nil, nil, ErrNotFound
}

// hasChildren checks if a directory has any children
func hasChildren(children *bbolt.Bucket, id []byte) bool {
	cursor := children.Cursor()
	k, _ := cursor.Seek(id)
	return len(k) > 0 && bytes.HasPrefix(k, id)
}
