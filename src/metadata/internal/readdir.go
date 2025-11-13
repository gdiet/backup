package internal

import (
	"backup/src/util"
	"bytes"

	"go.etcd.io/bbolt"
)

// ReaddirForID lists the entries under the specified parent directory. It does not check whether the parent exists.
func ReaddirForID(tree, children *bbolt.Bucket, parentID []byte) (entries []TreeEntry, err error) {
	cursor := children.Cursor()
	parentPrefix := parentID

	// Iterate through children
	for k, _ := cursor.Seek(parentPrefix); len(k) > 0; k, _ = cursor.Next() {
		// Check if this key still belongs to our parent
		if !bytes.HasPrefix(k, parentPrefix) {
			break // No more children for this parent
		}
		util.Assert(len(k) == 16, "invalid child key length")
		entry, err := treeEntry(tree, k[8:16])
		if err != nil {
			util.AssertionFailedf("invalid tree entry for child ID %x", k[8:16])
			continue // Orphaned child reference, skip
		}
		entries = append(entries, entry)
	}

	return entries, nil
}
