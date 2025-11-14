package internal

import "go.etcd.io/bbolt"

// Lookup resolves a path (array of tree entry names) to both ID and TreeEntry.
// Returns os.ErrNotExist if any component of the path does not exist.
// An empty path returns the root directory (ID 0 with synthetic root entry).
func Lookup(tree *bbolt.Bucket, children Bucket, path []string) ([]byte, TreeEntry, error) {
	if len(path) == 0 {
		rootID := make([]byte, 8) // root id = 0 (as 8 bytes)
		return rootID, NewDirEntry(""), nil
	}

	id := make([]byte, 8) // root id = 0 (as 8 bytes)
	var finalEntry TreeEntry

	for _, component := range path {
		childID, entry, err := getChild(tree, children, id, component)
		if err != nil {
			return nil, nil, err
		}
		id = childID
		finalEntry = entry
	}

	return id, finalEntry, nil
}
