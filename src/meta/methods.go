package meta

import (
	"backup/src/fserr"
	"bytes"
	"errors"

	"go.etcd.io/bbolt"
)

// lookup resolves a path (array of tree entry names) to both ID and TreeEntry.
// Returns ErrNotFound if any component of the path does not exist.
// An empty path returns the root directory (ID 0 with synthetic root entry).
func lookup(tree *bbolt.Bucket, children *bbolt.Bucket, path []string) (id []byte, entry TreeEntry, err error) {
	id = make([]byte, 8) // root ID is 0 (as 8 bytes)
	if len(path) == 0 {
		return id, NewDirEntry(""), nil
	}
	for _, component := range path {
		id, entry, err = getChild(tree, children, id, component)
		if err != nil {
			return nil, nil, err
		}
	}
	return id, entry, nil
}

// getChild searches for a child with the given name under the specified parent.
// Returns the child ID as bytes and the tree entry.
// Returns NotFound if the child does not exist.
func getChild(tree *bbolt.Bucket, children *bbolt.Bucket, parentID []byte, name string) ([]byte, TreeEntry, error) {
	cursor := children.Cursor()
	for k, _ := cursor.Seek(parentID); len(k) > 0; k, _ = cursor.Next() {
		if !bytes.HasPrefix(k, parentID) {
			break // No more children for this parent
		}

		if len(k) != 16 {
			return nil, nil, fserr.IO
		}

		childID := k[8:16]
		entry, err := treeEntryFromBytes(tree.Get(childID))
		if err != nil {
			return nil, nil, err
		}

		if entry.Name() == name {
			return childID, entry, nil
		}
	}
	return nil, nil, fserr.NotFound
}

// Mkdir creates a new directory. It does not check whether the parent exists.
// Returns the ID of the newly created directory as bytes.
// Returns os.ErrExist if a child with the same name already exists under the specified parent.
func mkdir(tree *bbolt.Bucket, children *bbolt.Bucket, parentID []byte, name string) ([]byte, error) {
	// Check if child with name already exists
	_, _, err := getChild(tree, children, parentID, name)
	if err == nil {
		return nil, fserr.Exists
	}
	if !errors.Is(err, fserr.NotFound) {
		return nil, err // Other error occurred
	}

	nextID, err := nextTreeID(tree)
	if err != nil {
		return nil, err
	}
	dirEntry := NewDirEntry(name)
	if err = tree.Put(nextID, dirEntry.ToBytes()); err != nil {
		return nil, err
	}

	err = addChild(children, parentID, nextID)
	if err != nil {
		return nil, err
	}
	return nextID, nil
}

// nextTreeID returns the next available tree entry ID as bytes.
// Starts from 1. Tree ID 0 is for root.
func nextTreeID(tree *bbolt.Bucket) ([]byte, error) {
	id, err := tree.NextSequence()
	return U64b(id), err
}

// addChild adds a child relationship between parentID and id.
func addChild(children *bbolt.Bucket, parentID []byte, id []byte) error {
	key := make([]byte, 16)
	copy(key[0:8], parentID)
	copy(key[8:16], id)
	return children.Put(key, []byte{})
}
