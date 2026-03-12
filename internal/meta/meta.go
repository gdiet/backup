package meta

import (
	"errors"
	"math"
	"path/filepath"

	"github.com/gdiet/backup/internal/fserr"
	"github.com/gdiet/backup/internal/util"
	"go.etcd.io/bbolt"
)

const RootID64 uint64 = 0

// FIXME check usage
var rootId = u64b(RootID64)
var rootEntry = &DirEntry{TreeEntry{rootId, ""}}

type Metadata struct {
	db           *bbolt.DB
	treeKey      []byte
	childrenKey  []byte
	dataKey      []byte
	freeAreasKey []byte
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
		// Create buckets if needed
		for _, bucketKey := range [][]byte{m.treeKey, m.childrenKey, m.dataKey, m.freeAreasKey} {
			if _, err := tx.CreateBucketIfNotExists(bucketKey); err != nil {
				return fserr.IO()
			}
		}
		// Initialize free areas with 0 -> MaxInt64 if needed
		freeAreas := tx.Bucket(m.freeAreasKey)
		firstKey, _ := freeAreas.Cursor().First()
		if len(firstKey) == 0 {
			if err := freeAreas.Put(u64b(0), u64b(math.MaxInt64)); err != nil {
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

// Lookup looks up a path and returns the ID and tree entry.
// Returns NotFound if the path does not exist.
func (m *Metadata) Lookup(path []string) (entry TreeEntryIface, err error) {
	err = m.viewTreeChildren(func(tree, children *bbolt.Bucket) error {
		entry, err = lookup(tree, children, path)
		return err
	})
	return entry, err
}

// GetChild looks up a child by name under the specified parent ID.
// Returns NotFound if the parent or the child does not exist.
func (m *Metadata) GetChild(parentID []byte, name string) (entry TreeEntryIface, err error) {
	err = m.viewTreeChildren(func(tree, children *bbolt.Bucket) error {
		entry, err = getChild(tree, children, parentID, name)
		return err
	})
	return entry, err
}

// Mkdir creates a new directory.
// Returns the ID of the newly created directory.
// Returns Exists if a child with the same name already exists under the specified parent.
// Returns NotFound if the parent directory does not exist.
// Returns NotDir if the parent is not a directory.
func (m *Metadata) Mkdir(path []string) (id []byte, err error) {
	if len(path) == 0 {
		return nil, fserr.Exists // Can't create root directory
	}

	err = m.updateTreeChildren(func(tree, children *bbolt.Bucket) error {
		parent, err := lookup(tree, children, path[:len(path)-1])
		switch {
		case err != nil:
			return err
		case parent.(*DirEntry) == nil:
			return fserr.NotDir
		}
		id, err = mkdir(tree, children, parent.ID(), path[len(path)-1])
		return err
	})
	return id, err
}

// Readdir lists the entries under the specified directory (nil or empty for root).
// Returns NotFound if the directory does not exist.
// Returns NotDir if the path is not a directory.
// Can return other errors.
func (m *Metadata) Readdir(path []string) (entries []TreeEntryIface, err error) {
	err = m.viewTreeChildren(func(tree, children *bbolt.Bucket) error {
		entry, err := lookup(tree, children, path)
		switch {
		case err != nil:
			return err
		case entry.(*DirEntry) == nil:
			return fserr.NotDir
		}
		entries, err = readdir(tree, children, entry.ID())
		return err
	})
	return entries, err
}

// Rmdir removes a directory.
// Returns NotFound if the path does not exist.
// Returns NotDir if the path is not a directory.
// Returns NotEmpty if the directory is not empty.
// Returns IsRoot if the directory is the root.
func (m *Metadata) Rmdir(path []string) error {
	if len(path) == 0 {
		return fserr.IsRoot // Can't remove root directory
	}

	return m.db.Update(func(tx *bbolt.Tx) error {
		tree := tx.Bucket(m.treeKey)
		children := tx.Bucket(m.childrenKey)

		parent, err := lookup(tree, children, path[:len(path)-1])
		if err != nil {
			return err // NotFound
		}
		entry, err := getChild(tree, children, parent.ID(), path[len(path)-1])
		if err != nil {
			return err // NotFound
		}
		if _, isDir := entry.(*DirEntry); !isDir {
			return fserr.NotDir // Test coverage: needs file implementation
		}

		return rmdir(tree, children, parent.ID(), entry.ID()) // nil or NotEmpty
	})
}

// Rename renames a file or directory, moving it to a new location if required.
// If oldPath is a directory and newPath is an empty directory, newPath is replaced.
// If oldPath is a file and newPath is an existing file, newPath is replaced.
// If oldPath and newPath exist and are the same, no operation is performed (success).
// Returns NotFound if the source path or a parent of the destination path does not exist.
// Returns NotEmpty if trying to rename a directory to an existing non-empty directory.
// Returns NotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
// Returns IsDir if trying to rename a file to a directory.
// Returns Invalid if trying to rename a directory to a subdirectory of itself.
// Returns IsRoot if trying to rename the root directory itself.
func (m *Metadata) Rename(oldPath []string, newPath []string) error {
	// handle root directory rename
	if stopHere, err := checkForRootDirectoryRename(oldPath, newPath); stopHere {
		return err
	}

	return m.db.Update(func(tx *bbolt.Tx) error {
		tree := tx.Bucket(m.treeKey)
		children := tx.Bucket(m.childrenKey)

		// Lookup source: parent and entry
		oldParent, err := lookup(tree, children, oldPath[:len(oldPath)-1])
		if err != nil {
			return err // Returns NotFound if the source path or a parent of the destination path does not exist.
		}
		oldEntry, err := getChild(tree, children, oldParent.ID(), oldPath[len(oldPath)-1])
		if err != nil {
			return err // Returns NotFound if the source path or a parent of the destination path does not exist.
		}

		// Check for no-op rename (source = target) for existing paths
		if equals(oldPath, newPath) {
			return nil // If oldPath and newPath exist and are the same, no operation is performed (success).
		}

		// Lookup destination: parent and, if any, entry
		newParent, err := lookup(tree, children, newPath[:len(newPath)-1])
		if err != nil {
			return err // Returns NotFound if the source path or a parent of the destination path does not exist.
		}
		// Ensure the new parent is a directory
		switch newParent.(type) {
		case *FileEntry:
			return fserr.NotDir // Returns NotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
		case *DirEntry:
			// continue
		default:
			util.AssertionFailedf("unexpected destination parent entry type %T in Rename", newParent)
			return fserr.NotDir // Returns NotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
		}

		// FIXME superfluous check
		if _, isDir := newParent.(*DirEntry); !isDir {
			return fserr.NotDir // Returns NotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
		}

		// Detect loop renames (directory to its own subdirectory)
		if len(oldPath) < len(newPath) && equals(oldPath, newPath[:len(oldPath)]) {
			return fserr.Invalid // Returns Invalid if trying to rename a directory to a subdirectory of itself.
		}

		switch oldEntry.(type) {
		case *FileEntry:
			// Returns IsDir if trying to rename a file to a directory.
			return errors.New("not implemented: renaming files") // TODO wait for implementation of files in the filesystem
		case *DirEntry:
			// continue
		default:
			util.AssertionFailedf("unexpected source entry type %T in Rename", oldEntry)
			return errors.New("invalid entry type") // Should not happen
		}

		// FIXME superfluous check
		oldDir, isDir := oldEntry.(*DirEntry)
		if !isDir {
			return fserr.NotDir // Returns NotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
		}

		return renameDirectory(tree, children, oldParent.ID(), oldDir, newParent.ID(), newPath[len(newPath)-1])
	})
}
