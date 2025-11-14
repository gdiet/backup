package internal

import (
	"backup/src/util"
	"bytes"
	"os"
	"syscall"

	"go.etcd.io/bbolt"
)

// Unlink deletes a file or directory from the filesystem.
// Returns os.ErrNotExist if the entry does not exist.
// Returns syscall.ENOTEMPTY if trying to delete a non-empty directory.
func Unlink(tree, children, data, freeAreas *bbolt.Bucket, idBytes []byte) error {

	// Check if entry exists
	entryBytes := tree.Get(idBytes)
	if entryBytes == nil {
		return os.ErrNotExist
	}

	// Parse the entry to determine type
	entry, err := treeEntryFromBytes(entryBytes)
	if err != nil {
		util.AssertionFailedf("invalid tree entry for ID %x", idBytes)
		return err
	}

	// Check if it's a directory and if it has children
	switch entry.(type) {
	case *DirEntry:
		// For directories, check if they have children
		if hasChildren(children, idBytes) {
			return syscall.ENOTEMPTY
		}
	case *FileEntry:
		// For files, decrement reference count and possibly free data
		fileEntry := entry.(*FileEntry)
		err := decrementDataReference(data, freeAreas, fileEntry.dref)
		if err != nil {
			return err
		}
	}

	// Remove all parent-child relationships where this entry is the child
	err = removeChildRelationships(children, idBytes)
	if err != nil {
		return err
	}

	// Remove the tree entry itself
	return tree.Delete(idBytes)
}

// removeChildRelationships removes all parent-child relationships where the given ID is the child
func removeChildRelationships(children *bbolt.Bucket, childBytes []byte) error {
	cursor := children.Cursor()

	// Collect all keys to delete (can't modify during iteration)
	var keysToDelete [][]byte

	for k, _ := cursor.First(); len(k) > 0; k, _ = cursor.Next() {
		util.Assert(len(k) == 16, "invalid child key length")

		// Check if the last 8 bytes (child ID part) match our target
		if bytes.Equal(k[8:16], childBytes) {
			// Make a copy of the key since bbolt data is only valid during transaction
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			keysToDelete = append(keysToDelete, keyCopy)
		}
	}

	// Delete all found relationships
	for _, key := range keysToDelete {
		err := children.Delete(key)
		if err != nil {
			return err
		}
	}

	return nil
}

// decrementDataReference decrements the reference count for a data entry and frees it if count reaches zero
func decrementDataReference(data, freeAreas *bbolt.Bucket, dref [40]byte) error {
	// Get current data entry
	entryBytes := data.Get(dref[:])
	if entryBytes == nil {
		// Data entry doesn't exist - this could be an orphaned file entry
		// In a robust system, we might want to log this as a warning
		return nil
	}

	dataEntry, err := DataEntryFromBytes(entryBytes)
	if err != nil {
		util.AssertionFailedf("invalid data entry for dref %x", dref)
		return err
	}

	// Decrement reference count
	dataEntry.Refs--

	if dataEntry.Refs == 0 {
		// Reference count reached zero, free the data areas and remove entry
		for _, area := range dataEntry.Areas {
			err := addFreeArea(freeAreas, area.Off, area.Len)
			if err != nil {
				return err
			}
		}

		// Remove the data entry
		return data.Delete(dref[:])
	} else {
		// Update the data entry with decremented reference count
		return data.Put(dref[:], dataEntry.ToBytes())
	}
}

// addFreeArea adds a free area to the free areas bucket (simplified implementation)
// In a full implementation, this should merge adjacent areas
func addFreeArea(freeAreas *bbolt.Bucket, offset, length uint64) error {
	offsetBytes := U64b(offset)
	lengthBytes := U64b(length)
	return freeAreas.Put(offsetBytes, lengthBytes)
}
