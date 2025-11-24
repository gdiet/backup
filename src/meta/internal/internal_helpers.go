package internal

import "backup/src/fserr"

// getNextTreeID returns the next available tree entry ID as bytes.
// Start from 1, since 0 is reserved for root.
func getNextTreeID(tree Bucket) ([]byte, error) {
	id, err := tree.B().NextSequence()
	return U64b(id), err
}

// treeEntry retrieves a TreeEntry by its ID bytes
// Returns ErrNotFound if the entry does not exist.
func treeEntry(tree Bucket, id []byte) (TreeEntry, error) {
	bytes := tree.B().Get(id)
	if bytes == nil {
		return nil, fserr.ErrNotFound
	}
	return treeEntryFromBytes(bytes)
}
