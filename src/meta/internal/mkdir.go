package internal

import "backup/src/fserr"

// Mkdir creates a new directory. It does not check whether the parent exists.
// Returns the ID of the newly created directory as bytes.
// Returns os.ErrExist if a child with the same name already exists under the specified parent.
func Mkdir(tree Bucket, children Bucket, parentID []byte, name string) ([]byte, error) {
	// Check if child with name already exists
	_, _, err := GetChild(tree, children, parentID, name)
	if err == nil {
		return nil, fserr.ErrExists
	}
	if err != fserr.ErrNotFound {
		return nil, err // Other error occurred
	}

	nextID, err := getNextTreeID(tree)
	if err != nil {
		return nil, err
	}
	dirEntry := NewDirEntry(name)
	if err = tree.Put(nextID, dirEntry.ToBytes()); err != nil {
		return nil, err
	}

	err = AddChild(children, parentID, nextID)
	if err != nil {
		return nil, err
	}
	return nextID, nil
}
