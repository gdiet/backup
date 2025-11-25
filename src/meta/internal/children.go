package internal

import (
	"backup/src/fserr"
	"bytes"

	"go.etcd.io/bbolt"
)

// Helper functions to manage parent-child relationships in the children bucket.

// AddChild adds a child relationship between parentID and id.
func AddChild(children Bucket, parentID []byte, id []byte) error {
	key := make([]byte, 16)
	copy(key[0:8], parentID)
	copy(key[8:16], id)
	return children.Put(key, []byte{})
}

// RemoveChild removes the child relationship between parentID and id.
func RemoveChild(children Bucket, parentID []byte, id []byte) error {
	key := make([]byte, 16)
	copy(key[0:8], parentID)
	copy(key[8:16], id)
	return children.B().Delete(key)
}

// hasChildren checks if a directory has any children
func hasChildren(children *bbolt.Bucket, id []byte) bool {
	cursor := children.Cursor()
	k, _ := cursor.Seek(id)
	return len(k) > 0 && bytes.HasPrefix(k, id)
}

// GetChild searches for a child with the given name under the specified parent.
// Returns the child ID as bytes and the tree entry.
// Returns NotFound if the child does not exist.
// TODO move to separate file, including tests
func GetChild(tree Bucket, children Bucket, parentID []byte, name string) ([]byte, TreeEntry, error) {
	cursor := children.B().Cursor()
	for k, _ := cursor.Seek(parentID); len(k) > 0; k, _ = cursor.Next() {
		if !bytes.HasPrefix(k, parentID) {
			break // No more children for this parent
		}

		if len(k) != 16 {
			return nil, nil, &fserr.DeserializationError{Msg: "invalid child key length"}
		}

		childID := k[8:16]
		entry, err := treeEntryFromBytes(tree.B().Get(childID))
		if err != nil {
			return nil, nil, err
		}

		if entry.Name() == name {
			return childID, entry, nil
		}
	}
	return nil, nil, fserr.NotFound
}
