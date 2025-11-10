package internal

import (
	"backup/src/util"
	"bytes"
	"os"

	"go.etcd.io/bbolt"
)

// Mkdir creates a new directory. It does not check whether the parent exists.
// Returns os.ErrExist if a child with the same name already exists under the specified parent.
func Mkdir(tree, children *bbolt.Bucket, parent uint64, name string) error {
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
			return err
		}
		if entry.GetName() == name {
			return os.ErrExist
		}
	}

	// get next available tree entries ID
	var nextID uint64
	if bytes, _ := tree.Cursor().Last(); bytes != nil {
		nextID = B64u(bytes) + 1
	}

	// write new dir entry
	dirEntry := NewDirEntry(name)
	err := tree.Put(U64b(nextID), dirEntry.ToBytes())
	if err != nil {
		return err
	}

	// create parent-child relationship
	childKey := make([]byte, 16)
	U64w(childKey, parent)
	U64w(childKey[8:], nextID)
	return children.Put(childKey, []byte{})
}
