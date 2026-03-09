package meta

import (
	"bytes"
	"errors"

	"github.com/gdiet/backup/internal/fserr"
	"github.com/gdiet/backup/internal/util"
	"go.etcd.io/bbolt"
)

// viewTreeChildren runs a read-only transaction providing the tree and children buckets.
func (m *Metadata) viewTreeChildren(fn func(tree, children *bbolt.Bucket) error) error {
	return m.db.View(func(tx *bbolt.Tx) error {
		return fn(tx.Bucket(m.treeKey), tx.Bucket(m.childrenKey))
	})
}

// updateTreeChildren runs a write transaction providing the tree and children buckets.
func (m *Metadata) updateTreeChildren(fn func(tree, children *bbolt.Bucket) error) error {
	return m.db.Update(func(tx *bbolt.Tx) error {
		return fn(tx.Bucket(m.treeKey), tx.Bucket(m.childrenKey))
	})
}

// lookup resolves a path (array of tree entry names) to both ID and TreeProp.
// Returns NotFound if any component of the path does not exist.
// An empty path returns the root directory (ID 0 with synthetic root entry).
func lookup(tree, children *bbolt.Bucket, path []string) (id []byte, entry TreeProp, err error) {
	id = RootId
	if len(path) == 0 {
		return id, newDirProp(""), nil
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
// Returns the child ID and the tree entry.
// Returns NotFound if the parent or the child does not exist.
func getChild(tree, children *bbolt.Bucket, parentID []byte, name string) ([]byte, TreeProp, error) {
	cursor := children.Cursor()
	for k, _ := cursor.Seek(parentID); len(k) > 0; k, _ = cursor.Next() {
		if !bytes.HasPrefix(k, parentID) {
			break // No more children for this parent
		}
		if len(k) != 16 {
			util.AssertionFailed("invalid child key length")
			return nil, nil, fserr.IO()
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

// mkdir creates a new directory. It does not check whether the parent exists or is a directory.
// Returns the ID of the newly created directory.
// Returns os.ErrExist if a child with the same name already exists under the specified parent.
func mkdir(tree, children *bbolt.Bucket, parentID []byte, name string) ([]byte, error) {
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
	dirEntry := newDirProp(name)
	if err = tree.Put(nextID, dirEntry.ToBytes()); err != nil {
		return nil, err
	}

	err = addChild(children, parentID, nextID)
	if err != nil {
		return nil, err
	}
	return nextID, nil
}

// nextTreeID returns the next available tree entry ID.
// Starts from 1. Tree ID 0 is for root, see RootID64 constant.
func nextTreeID(tree *bbolt.Bucket) ([]byte, error) {
	id, err := tree.NextSequence()
	return u64b(id), err
}

// addChild adds a child relationship between parentID and id.
func addChild(children *bbolt.Bucket, parentID []byte, id []byte) error {
	key := make([]byte, 16)
	copy(key[0:8], parentID)
	copy(key[8:16], id)
	err := children.Put(key, []byte{})
	util.Assertf(err == nil, "bbolt put failed: %v", err)
	return err
}

// readdir lists the entries under the specified parent directory. It does not check whether the parent exists.
func readdir(tree, children *bbolt.Bucket, parentID []byte) (entries []TreeProp, err error) {
	cursor := children.Cursor()
	for k, _ := cursor.Seek(parentID); len(k) > 0; k, _ = cursor.Next() {
		if !bytes.HasPrefix(k, parentID) {
			break // No more children for this parent
		}
		if len(k) != 16 {
			util.AssertionFailed("invalid child key length")
			return nil, fserr.IO()
		}
		entry, err := treeEntry(tree, k[8:16])
		if err != nil {
			util.AssertionFailedf("orphaned child reference: parent ID %x / child ID %x", parentID, k[8:16])
			continue // Skip orphaned child reference
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// treeEntry retrieves a TreeProp by its ID bytes
// Returns NotFound if the entry does not exist.
func treeEntry(tree *bbolt.Bucket, id []byte) (TreeProp, error) {
	bytes := tree.Get(id)
	if bytes == nil {
		return nil, fserr.NotFound
	}
	return treeEntryFromBytes(bytes)
}

// Rmdir removes a directory specified by its ID under the given parent ID.
// It does not check whether id is actually a directory.
// Returns NotEmpty if the directory has children.
func rmdir(tree, children *bbolt.Bucket, parentID []byte, id []byte) error {
	// Check if directory has children
	if hasChildren(children, id) {
		return fserr.NotEmpty
	}

	// Remove the directory entry from the tree
	if err := tree.Delete(id); err != nil {
		return err
	}

	return removeChild(children, parentID, id)
}

// hasChildren checks if a directory has any children
func hasChildren(children *bbolt.Bucket, id []byte) bool {
	cursor := children.Cursor()
	k, _ := cursor.Seek(id)
	return len(k) > 0 && bytes.HasPrefix(k, id)
}

// removeChild removes the child relationship between parentID and id.
func removeChild(children *bbolt.Bucket, parentID []byte, id []byte) error {
	key := make([]byte, 16)
	copy(key[0:8], parentID)
	copy(key[8:16], id)
	err := children.Delete(key)
	util.Assertf(err == nil, "bbolt delete failed: %v", err)
	return err
}

// checkForRootDirectoryRename checks if the rename operation involves the root directory.
// If oldPath is the root directory, it returns IsRoot unless newPath is also the root directory (no-op in that case).
func checkForRootDirectoryRename(oldPath []string, newPath []string) (bool, error) {
	if len(oldPath) == 0 {
		if len(newPath) == 0 {
			return true, nil // If oldPath and newPath exist and are the same, no operation is performed (success).
		}
		return true, fserr.IsRoot // Returns IsRoot if trying to rename the root directory itself.
	}
	return false, nil
}

// renameDirectory handles renaming of directories, including moving to a new parent.
// Returns NotEmpty if trying to rename a directory to an existing non-empty directory.
// Returns NotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
func renameDirectory(
	tree *bbolt.Bucket, children *bbolt.Bucket,
	oldParentID, oldEntryID []byte, oldEntry TreeProp,
	newParentID []byte, newEntryName string) error {

	// Lookup destination entry to replace, if any
	replaceEntryID, replaceEntry, getReplaceEntryError := getChild(tree, children, newParentID, newEntryName)

	if getReplaceEntryError == nil {
		// Destination exists
		switch replaceEntry.(type) {
		case *FileProp:
			return fserr.NotDir // Returns NotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
		case *DirProp:
			// Remove destination entry unless it's not empty
			err := rmdir(tree, children, newParentID, replaceEntryID)
			if err != nil {
				return err // Returns NotEmpty if trying to rename a directory to an existing non-empty directory.
			}
		default:
			util.AssertionFailedf("unexpected destination entry type %T in Rename", replaceEntry)
			return errors.New("invalid entry type") // Should not happen
		}
	}

	// Move entry to new location
	if err := removeChild(children, oldParentID, oldEntryID); err != nil {
		return err
	}
	if err := addChild(children, newParentID, oldEntryID); err != nil {
		return err
	}

	// Rename to the new name if necessary
	if oldEntry.Name() != newEntryName {
		newEntry := newDirProp(newEntryName)
		err := tree.Put(oldEntryID, newEntry.ToBytes())
		util.Assertf(err == nil, "bbolt put failed: %v", err)
		return err
	}
	return nil
}
