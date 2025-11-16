package internal

// Lookup resolves a path (array of tree entry names) to both ID and TreeEntry.
// Returns ErrNotFound if any component of the path does not exist.
// An empty path returns the root directory (ID 0 with synthetic root entry).
func Lookup(tree Bucket, children Bucket, path []string) (id []byte, entry TreeEntry, err error) {
	id = make([]byte, 8) // root ID is 0 (as 8 bytes)
	if len(path) == 0 {
		return id, NewDirEntry(""), nil
	}
	for _, component := range path {
		id, entry, err = GetChild(tree, children, id, component)
		if err != nil {
			return nil, nil, err
		}
	}
	return id, entry, nil
}
