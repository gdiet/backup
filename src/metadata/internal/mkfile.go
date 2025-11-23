package internal

import "time"

// Mkfile creates a new file. It does not check whether the parent exists.
// Returns the ID of the newly created file.
// Returns os.ErrExist if a child with the same name already exists under the specified parent.
func Mkfile(tree Bucket, children Bucket, parentID []byte, name string) ([]byte, error) {
	// Check if child with name already exists
	_, _, err := GetChild(tree, children, parentID, name)
	if err == nil {
		return nil, ErrExists
	}
	if err != ErrNotFound {
		return nil, err // Other error occurred
	}

	nextID, err := getNextTreeID(tree)
	if err != nil {
		return nil, err
	}
	dref := [40]byte{} // empty dref for new file
	fileEntry := NewFileEntry(name, time.Now().UnixMilli(), dref)
	if err = tree.Put(nextID, fileEntry.ToBytes()); err != nil {
		return nil, err
	}

	err = AddChild(children, parentID, nextID)
	if err != nil {
		return nil, err
	}
	return nextID, nil
}
