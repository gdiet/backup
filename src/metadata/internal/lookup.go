package internal

import (
	"backup/src/util"
	"bytes"
	"os"

	"go.etcd.io/bbolt"
)

// Lookup resolves a path (array of directory names) to a directory ID.
// Returns os.ErrNotExist if any component of the path does not exist.
// An empty path returns the root directory ID (0).
func Lookup(tree, children *bbolt.Bucket, path []string) (uint64, error) {
	// Start at root
	currentID := uint64(0)

	// Traverse each component of the path
	for _, component := range path {
		// Find the child with the given name under currentID
		childID, err := findChild(tree, children, currentID, component)
		if err != nil {
			return 0, err
		}

		// Check that the found entry is actually a directory
		entryBytes := tree.Get(U64b(childID))
		if entryBytes == nil {
			// This should not happen if findChild worked correctly
			util.AssertionFailedf("tree entry disappeared for ID %x", U64b(childID))
			return 0, os.ErrNotExist
		}

		entry, err := treeEntryFromBytes(entryBytes)
		if err != nil {
			util.AssertionFailedf("invalid tree entry for ID %x", U64b(childID))
			return 0, err
		}

		// Ensure it's a directory (not a file)
		if _, isDir := entry.(*DirEntry); !isDir {
			return 0, os.ErrNotExist // Path component is a file, not directory
		}

		currentID = childID
	}

	return currentID, nil
}

// findChild searches for a child with the given name under the specified parent.
// Returns the child ID or os.ErrNotExist if not found.
func findChild(tree, children *bbolt.Bucket, parentID uint64, name string) (uint64, error) {
	cursor := children.Cursor()
	parentPrefix := U64b(parentID)

	// Iterate through children of the parent
	for k, _ := cursor.Seek(parentPrefix); len(k) > 0; k, _ = cursor.Next() {
		// Check if this key still belongs to our parent
		if !bytes.HasPrefix(k, parentPrefix) {
			break // No more children for this parent
		}

		util.Assert(len(k) == 16, "invalid child key length")
		childID := k[8:16] // Extract child ID from key

		// Get the tree entry for this child
		entryBytes := tree.Get(childID)
		if entryBytes == nil {
			// Orphaned child reference, skip
			continue
		}

		// Parse the tree entry
		entry, err := treeEntryFromBytes(entryBytes)
		if err != nil {
			util.AssertionFailedf("invalid tree entry for child ID %x", childID)
			return 0, err
		}

		// Check if this is the child we're looking for
		if entry.GetName() == name {
			return B64u(childID), nil
		}
	}

	// Child not found
	return 0, os.ErrNotExist
}
