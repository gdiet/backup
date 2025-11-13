package internal

import (
	"backup/src/util"
	"bytes"
	"os"

	"go.etcd.io/bbolt"
)

// Mkdir creates a new directory. It does not check whether the parent exists.
// Returns the ID of the newly created directory.
// Returns os.ErrExist if a child with the same name already exists under the specified parent.
func Mkdir(tree, children *bbolt.Bucket, parent uint64, name string) (uint64, error) {
	// check if child with name exists
	cursor := children.Cursor()
	parentPrefix := U64b(parent)
	for k, _ := cursor.Seek(parentPrefix); len(k) > 0; k, _ = cursor.Next() {
		// Check if this key still belongs to our parent
		if !bytes.HasPrefix(k, parentPrefix) {
			break // No more children for this parent
		}

		util.Assert(len(k) == 16, "invalid child key length")
		entry, err := treeEntry(tree, k[8:16])
		if err != nil {
			util.AssertionFailedf("invalid tree entry for child ID %x", k[8:16])
			return 0, err
		}
		if entry.GetName() == name {
			return 0, os.ErrExist
		}
	}

	// Get next available tree entries ID. Start from 1, since 0 is reserved for root.
	var nextID uint64 = 1
	if bytes, _ := tree.Cursor().Last(); bytes != nil {
		nextID = B64u(bytes) + 1
	}

	// write new dir entry
	dirEntry := NewDirEntry(name)
	err := tree.Put(U64b(nextID), dirEntry.ToBytes())
	if err != nil {
		return 0, err
	}

	// create parent-child relationship
	childKey := make([]byte, 16)
	U64w(childKey, parent)
	U64w(childKey[8:], nextID)
	err = children.Put(childKey, []byte{})
	if err != nil {
		return 0, err
	}
	return nextID, nil
}
