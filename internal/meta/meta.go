package meta

import (
	"bytes"
	"errors"
	"math"
	"path/filepath"

	"github.com/gdiet/backup/internal/fserr"
	"github.com/gdiet/backup/internal/util"
	"go.etcd.io/bbolt"
)

var RootEntry = &DirEntry{i64b(0), ""}

type Metadata struct {
	db                                          *bbolt.DB
	treeKey, childrenKey, dataKey, freeAreasKey []byte
}

type Context struct {
	tree, children, data, freeAreas *bbolt.Bucket
}

func NewMetadata(repository string) (*Metadata, error) {
	dbPath := filepath.Join(repository, "dedupfs.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}
	m := &Metadata{
		db:           db,
		treeKey:      []byte("tree"),
		childrenKey:  []byte("children"),
		dataKey:      []byte("data"),
		freeAreasKey: []byte("free_areas"),
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		// Create Context if needed
		for _, bucketKey := range [][]byte{m.treeKey, m.childrenKey, m.dataKey, m.freeAreasKey} {
			if _, err := tx.CreateBucketIfNotExists(bucketKey); err != nil {
				return fserr.IO()
			}
		}
		// Initialize free areas with 0 -> MaxInt64 if needed
		freeAreas := tx.Bucket(m.freeAreasKey)
		firstKey, _ := freeAreas.Cursor().First()
		if len(firstKey) == 0 {
			if err := freeAreas.Put(i64b(0), i64b(math.MaxInt64)); err != nil {
				return fserr.IO()
			}
		}
		return nil
	})
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return m, nil
}

// Close closes the metadata repository.
func (m *Metadata) Close() error {
	return m.db.Close()
}

// Read sets up a read transaction.
func (m *Metadata) Read(fn func(context *Context) error) error {
	return m.db.View(func(tx *bbolt.Tx) error {
		return fn(m.newContext(tx))
	})
}

// Write sets up a write transaction.
func (m *Metadata) Write(fn func(context *Context) error) error {
	return m.db.Update(func(tx *bbolt.Tx) error {
		return fn(m.newContext(tx))
	})
}

// Lookup looks up a path.
// Returns NotFound if the path does not exist.
func (c *Context) Lookup(path []string) (TreeEntry, error) {
	var err error
	var entry TreeEntry = RootEntry
	for _, component := range path {
		entry, err = c.GetChild(entry.ID(), component)
		if err != nil {
			return nil, err // Path not found or other error
		}
	}
	return entry, nil // Success
}

// GetChild looks up a child by name.
// Returns NotFound if the parent or the child do not exist.
func (c *Context) GetChild(parentID []byte, name string) (TreeEntry, error) {
	cursor := c.children.Cursor()
	for k, _ := cursor.Seek(parentID); len(k) > 0; k, _ = cursor.Next() {
		if !bytes.HasPrefix(k, parentID) {
			break // No more children for this parent
		}
		if len(k) != 16 {
			return nil, fserr.IO() // Other error
		}
		childID := k[8:16]
		entry, err := treeEntryFrom(childID, c.tree.Get(childID))
		if err != nil {
			return nil, err // Other error
		}
		if entry.Name() == name {
			return entry, nil // Success
		}
	}
	return nil, fserr.NotFound // Parent or child not found
}

// Mkdir creates a new directory and returns its ID.
// Returns Exists if a child with the name already exists.
// Returns NotFound if the parent does not exist.
// Returns NotDir if the parent is not a directory.
func (c *Context) Mkdir(path []string) ([]byte, error) {
	if len(path) == 0 {
		return nil, fserr.Exists // Can't create root directory
	}
	parent, err := c.Lookup(path[:len(path)-1])
	if err != nil {
		return nil, err // Parent not found or other error
	}
	if _, isDir := parent.(*DirEntry); !isDir {
		return nil, fserr.NotDir // Parent not a directory
	}
	name := path[len(path)-1]
	return c.MkdirUnchecked(parent.ID(), name) // Success or ...
}

// MkdirUnchecked creates a new directory without validating the parent ID.
// Returns Exists if a child with the name already exists.
func (c *Context) MkdirUnchecked(parentID []byte, name string) ([]byte, error) {
	_, err := c.GetChild(parentID, name)
	if err == nil {
		return nil, fserr.Exists // Child already exists
	}
	if !errors.Is(err, fserr.NotFound) {
		return nil, err // Other error
	}
	id, err := c.nextTreeID()
	if err != nil {
		return nil, err // Other error
	}
	err = c.tree.Put(id, (&DirEntry{name: name}).ToBytes())
	if err != nil {
		return nil, err // Other error
	}
	err = c.addChild(parentID, id)
	if err != nil {
		return nil, err // Other error
	}
	return id, nil // Success
}

// Mkdirs creates all directories along the path if missing.
// Returns NotDir if the path contains an entry that is not a directory.
func (c *Context) Mkdirs(path []string) ([]byte, error) {
	var err error
	id := RootEntry.ID()
	for _, name := range path {
		id, err = c.MkdirUnchecked(id, name)
		if errors.Is(err, fserr.Exists) {
			continue
		}
		if err != nil {
			return nil, err // Other error
		}
	}
	return id, nil
}

// Readdir lists directory entries.
// Returns NotFound if the directory does not exist.
// Returns NotDir if the path is not a directory.
func (c *Context) Readdir(path []string) ([]TreeEntry, error) {
	entry, err := c.Lookup(path)
	if err != nil {
		return nil, err // Path not found or other error
	}
	if _, isDir := entry.(*DirEntry); !isDir {
		return nil, fserr.NotDir // Path not a directory
	}
	var entries []TreeEntry
	cursor := c.children.Cursor()
	for k, _ := cursor.Seek(entry.ID()); len(k) > 0; k, _ = cursor.Next() {
		if !bytes.HasPrefix(k, entry.ID()) {
			break // No more children for this parent
		}
		if len(k) != 16 {
			return nil, fserr.IO() // Other error
		}
		child, err := c.treeEntry(k[8:16])
		if err != nil {
			util.AssertionFailedf("orphaned child reference: parent ID %x / child ID %x", entry.ID(), k[8:16])
			continue // Skip orphaned child reference
		}
		entries = append(entries, child)
	}
	return entries, nil // Success
}

// Rmdir removes a directory.
// Returns NotFound if the path does not exist.
// Returns NotDir if the path is not a directory.
// Returns NotEmpty if the directory is not empty.
// Returns IsRoot if the directory is the root.
func (c *Context) Rmdir(path []string) error {
	if len(path) == 0 {
		return fserr.IsRoot // Can't remove root directory
	}
	parent, err := c.Lookup(path[:len(path)-1])
	if err != nil {
		return err // Parent not found or ...
	}
	return c.removeDir(parent.ID(), path[len(path)-1]) // Success or ...
}

// Rename renames a file or directory, moving it to a new location if required.
// If oldPath is a directory and newPath is an empty directory, newPath is replaced.
// If oldPath is a file and newPath is an existing file, newPath is replaced.
// If oldPath and newPath exist and are the same, no operation is performed (success).
// Returns NotFound if the source path or a parent of the destination path does not exist.
// Returns NotEmpty if trying to rename a directory to an existing non-empty directory.
// Returns NotDir if the parent of the destination is not a directory or if trying to rename a directory to a file.
// Returns IsDir if trying to rename a file to a directory.
// Returns Invalid if trying to rename a directory to a subdirectory of itself.
// Returns IsRoot if trying to rename the root directory itself.
func (c *Context) Rename(oldPath []string, newPath []string) error {
	if len(oldPath) == 0 || len(newPath) == 0 {
		return fserr.IsRoot // Can't manipulate root directory
	}

	// Lookup source: parent and entry
	oldParent, err := c.Lookup(oldPath[:len(oldPath)-1])
	if err != nil {
		return err // Source parent not found
	}
	oldEntry, err := c.GetChild(oldParent.ID(), oldPath[len(oldPath)-1])
	if err != nil {
		return err // Source entry not found
	}

	// Check for no-op rename (source = target) - but only for existing path
	if equals(oldPath, newPath) {
		return nil // Success if oldPath and newPath exist and are the same
	}

	// Lookup destination parent
	newParent, err := c.Lookup(newPath[:len(newPath)-1])
	if err != nil {
		return err // Destination parent not found
	}
	if _, isDir := newParent.(*DirEntry); !isDir {
		return fserr.NotDir // Destination not a directory
	}

	// Detect loop renames (directory to its own subdirectory)
	if len(oldPath) < len(newPath) && equals(oldPath, newPath[:len(oldPath)]) {
		return fserr.Invalid // Loop rename
	}

	newName := newPath[len(newPath)-1]
	if _, isDir := oldEntry.(*DirEntry); isDir {
		return c.renameDirectory(oldParent.ID(), oldEntry, newParent.ID(), newName) // Success or ...
	}
	// Returns IsDir if trying to rename a file to a directory.
	return errors.New("not implemented: renaming files")
}
