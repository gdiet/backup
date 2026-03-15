package meta

import (
	"bytes"
	"errors"

	"github.com/gdiet/backup/internal/fserr"
	"github.com/gdiet/backup/internal/util"
	"go.etcd.io/bbolt"
)

func (m *Metadata) newContext(tx *bbolt.Tx) *Context {
	return &Context{
		tx.Bucket(m.treeKey), tx.Bucket(m.childrenKey), tx.Bucket(m.childrenKey), tx.Bucket(m.childrenKey),
	}
}

// nextTreeID returns the next available tree entry ID.
// Starts from 1. Tree ID 0 is for root, see RootEntry constant.
func (c *Context) nextTreeID() ([]byte, error) {
	id, err := c.tree.NextSequence()
	return i64b(int64(id)), err
}

// addChild adds a child relationship between parentID and id.
func (c *Context) addChild(parentID []byte, id []byte) error {
	key := make([]byte, 16)
	copy(key[0:8], parentID)
	copy(key[8:16], id)
	err := c.children.Put(key, []byte{})
	util.Assertf(err == nil, "bbolt put failed: %v", err)
	return err
}

// treeEntry retrieves a TreeEntry.
// Returns NotFound if the entry does not exist.
func (c *Context) treeEntry(id []byte) (TreeEntry, error) {
	if entryBytes := c.tree.Get(id); entryBytes == nil {
		return nil, fserr.NotFound
	} else {
		return treeEntryFrom(id, entryBytes)
	}
}

// removeChild removes the child relationship between parentID and id.
func (c *Context) removeChild(parentID, id []byte) error {
	key := make([]byte, 16)
	copy(key[0:8], parentID)
	copy(key[8:16], id)
	err := c.children.Delete(key)
	util.Assertf(err == nil, "bbolt delete failed: %v", err)
	return err
}

// removeDir removes a directory.
// Returns NotFound if the directory does not exist.
// Returns NotDir if the path is not a directory.
// Returns NotEmpty if the directory is not empty.
func (c *Context) removeDir(parentId []byte, name string) error {
	entry, err := c.GetChild(parentId, name)
	if err != nil {
		return err // NotFound or other error
	}
	_, isDir := entry.(*DirEntry)
	if !isDir {
		return fserr.NotDir // Path not a directory
	}
	cursor := c.children.Cursor()
	k, _ := cursor.Seek(entry.ID())
	if len(k) > 0 && bytes.HasPrefix(k, entry.ID()) {
		return fserr.NotEmpty // Directory not empty
	}
	err = c.tree.Delete(entry.ID())
	if err != nil {
		return err // Other error
	}
	return c.removeChild(parentId, entry.ID()) // Success or other error
}

// renameDirectory handles renaming of directories, including moving to a new parent.
// Returns NotEmpty if trying to rename a directory to an existing non-empty directory.
// Returns NotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
func (c *Context) renameDirectory(
	oldParentID []byte, oldEntry TreeEntry,
	newParentID []byte, newEntryName string) error {

	err := c.removeDir(newParentID, newEntryName)
	if err != nil && !errors.Is(err, fserr.NotFound) {
		return err // Target not a directory or not empty or ...
	}

	if !equals(oldParentID, newParentID) {
		// Move directory to new location
		err = c.removeChild(oldParentID, oldEntry.ID())
		if err != nil {
			return err // Other error
		}
		err = c.addChild(newParentID, oldEntry.ID())
		if err != nil {
			return err // Other error
		}
	}

	// Rename to the new name if necessary
	if oldEntry.Name() != newEntryName {
		return c.tree.Put(oldEntry.ID(), (&DirEntry{name: newEntryName}).ToBytes())
	}
	return nil
}
