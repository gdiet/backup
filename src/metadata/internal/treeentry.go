package internal

import (
	"bytes"
	"fmt"

	"go.etcd.io/bbolt"
)

// treeEntry retrieves a TreeEntry by its ID bytes
func treeEntry(tree *bbolt.Bucket, id []byte) (TreeEntry, error) {
	bytes := tree.Get(id)
	if bytes == nil { // TODO check - ENOTFOUND?
		return nil, fmt.Errorf("orphaned tree entry for ID %x", id)
	}
	return treeEntryFromBytes(bytes)
}

// hasChildren checks if a directory has any children
func hasChildren(children *bbolt.Bucket, id []byte) bool {
	cursor := children.Cursor()
	k, _ := cursor.Seek(id)
	return len(k) > 0 && bytes.HasPrefix(k, id)
}
