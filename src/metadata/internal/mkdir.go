package internal

import (
	"os"

	"go.etcd.io/bbolt"
)

// Mkdir creates a new directory. It does not check whether the parent exists.
// Returns the ID of the newly created directory as bytes.
// Returns os.ErrExist if a child with the same name already exists under the specified parent.
func Mkdir(tree, children *bbolt.Bucket, parentID []byte, name string) ([]byte, error) {
	// Check if child with name already exists
	_, _, err := getChild(tree, children, parentID, name)
	if err == nil {
		return nil, os.ErrExist // Child already exists
	}
	if err != os.ErrNotExist {
		return nil, err // Other error occurred
	}

	// Get next available tree entries ID. Start from 1, since 0 is reserved for root.
	var nextID uint64 = 1
	if bytes, _ := tree.Cursor().Last(); bytes != nil {
		nextID = B64u(bytes) + 1
	}
	nextIDBytes := U64b(nextID)

	// write new dir entry
	dirEntry := NewDirEntry(name)
	err = tree.Put(nextIDBytes, dirEntry.ToBytes())
	if err != nil {
		return nil, err
	}

	// create parent-child relationship
	childKey := make([]byte, 16)
	copy(childKey[0:8], parentID)
	copy(childKey[8:16], nextIDBytes)
	err = children.Put(childKey, []byte{})
	if err != nil {
		return nil, err
	}
	return nextIDBytes, nil
}
