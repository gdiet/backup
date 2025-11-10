package internal

import (
	"fmt"

	"go.etcd.io/bbolt"
)

func treeEntry(tree *bbolt.Bucket, id []byte) (TreeEntry, error) {
	bytes := tree.Get(id)
	if bytes == nil {
		return nil, fmt.Errorf("orphaned tree entry for child ID %x", id)
	}
	return TreeEntryFromBytes(bytes)
}
