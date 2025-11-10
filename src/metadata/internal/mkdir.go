package internal

import (
	"backup/src/util"
	"bytes"
	"os"

	"go.etcd.io/bbolt"
)

// Mkdir creates a new directory entry using the provided transaction and buckets.
// This function contains the core business logic and can be unit tested with 100% coverage.
// It checks for existing children with the same name and creates a new directory entry if none exists.
func Mkdir(tree, children *bbolt.Bucket, parent uint64, name string) error {
	// check if child with name exists
	cursor := children.Cursor()
	parentPrefix := U64b(parent)
	for k, _ := cursor.Seek(parentPrefix); len(k) >= 8; k, _ = cursor.Next() {
		// Check if this key still belongs to our parent
		if !bytes.HasPrefix(k, parentPrefix) {
			break // No more children for this parent
		}

		util.Assert(len(k) == 16, "invalid child key length")
		childID := k[8:16] // Extract child ID from key
		bytes := tree.Get(childID)
		if bytes == nil {
			continue
		}
		entry, err := TreeEntryFromBytes(bytes)
		if err != nil {
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
	// Key format: parentID(8 bytes) + childID(8 bytes) -> empty value
	childKey := make([]byte, 16)
	copy(childKey[0:8], U64b(parent))
	copy(childKey[8:16], U64b(nextID))
	return children.Put(childKey, []byte{})
}
