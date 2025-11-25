package internal

import "backup/src/fserr"

// Rmdir removes a directory specified by its ID under the given parent ID.
// It does not check whether id is actually a directory.
// Returns NotEmpty if the directory has children.
func Rmdir(tree Bucket, children Bucket, parentID []byte, id []byte) error {
	// Check if directory has children
	if hasChildren(children.B(), id) {
		return fserr.NotEmpty
	}

	// Remove the directory entry from the tree
	if err := tree.B().Delete(id); err != nil {
		return err
	}

	return RemoveChild(children, parentID, id)
}
