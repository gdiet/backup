package meta

import (
	"backup/src/fserr"
	"backup/src/util"
	"errors"
	"math"
	"path/filepath"

	"go.etcd.io/bbolt"
)

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
	m := &Metadata{db: db,
		treeKey:      []byte("tree"),
		childrenKey:  []byte("children"),
		dataKey:      []byte("data"),
		freeAreasKey: []byte("free_areas")}
	err = db.Update(func(tx *bbolt.Tx) error {
		// Create buckets if needed
		for _, bucketKey := range [][]byte{m.treeKey, m.childrenKey, m.dataKey, m.freeAreasKey} {
			_, err := tx.CreateBucketIfNotExists(bucketKey)
			if err != nil {
				return fserr.IO()
			}
		}
		// Initialize free areas if needed
		freeAreas := tx.Bucket(m.freeAreasKey)
		firstKey, _ := freeAreas.Cursor().First()
		if len(firstKey) == 0 {
			// Add initial free area: 0 -> MaxInt64
			if err := freeAreas.Put(U64b(0), U64b(math.MaxInt64)); err != nil {
				return fserr.IO()
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Metadata{
		db:           db,
		treeKey:      []byte("tree"),
		childrenKey:  []byte("children"),
		dataKey:      []byte("data"),
		freeAreasKey: []byte("free_areas"),
	}, nil
}

// Close closes the metadata repository.
func (m *Metadata) Close() error {
	return m.db.Close()
}

// Lookup looks up a path and returns the ID and tree entry.
// Returns NotFound if the path does not exist.
func (m *Metadata) Lookup(path []string) (id uint64, entry TreeEntry, err error) {
	err = m.db.View(func(tx *bbolt.Tx) error {
		tree := tx.Bucket(m.treeKey)
		children := tx.Bucket(m.childrenKey)
		var idBytes []byte
		if idBytes, entry, err = lookup(tree, children, path); err != nil {
			return err
		}
		id = B64u(idBytes)
		return nil
	})
	return id, entry, err
}

// Mkdir creates a new directory.
// Returns the ID of the newly created directory.
// Returns Exists if a child with the same name already exists under the specified parent.
// Returns NotFound if the parent directory does not exist.
// Returns NotDir if the parent is not a directory.
func (m *Metadata) Mkdir(path []string) (uint64, error) {
	if len(path) == 0 {
		return 0, fserr.Exists // Can't create root directory
	}

	var idBytes []byte
	err := m.db.Update(func(tx *bbolt.Tx) error {
		tree := tx.Bucket(m.treeKey)
		children := tx.Bucket(m.childrenKey)

		parentID, parent, err := lookup(tree, children, path[:len(path)-1])
		if err != nil {
			return err // NotFound
		}
		// Ensure the parent is a directory
		if _, isDir := parent.(*DirEntry); !isDir {
			return fserr.NotDir // Test coverage: needs file implementation
		}

		idBytes, err = mkdir(tree, children, parentID, path[len(path)-1])
		return err
	})
	if err != nil {
		return 0, err
	}
	return B64u(idBytes), nil
}

// Readdir lists the entries under the specified directory (nil or empty for root).
// Returns NotFound if the directory does not exist.
// Returns NotDir if the path is not a directory.
// Can return other errors.
func (m *Metadata) Readdir(path []string) (entries []TreeEntry, err error) {
	err = m.db.View(func(tx *bbolt.Tx) error {
		tree := tx.Bucket(m.treeKey)
		children := tx.Bucket(m.childrenKey)

		id, entry, err := lookup(tree, children, path)
		if err != nil {
			return err // NotFound and others
		}

		// Ensure the target is a directory
		if _, isDir := entry.(*DirEntry); !isDir {
			return fserr.NotDir // Test coverage: needs file implementation
		}

		// Read the directory contents
		entries, err = readdir(tree, children, id)
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

		parentID, _, err := lookup(tree, children, path[:len(path)-1])
		if err != nil {
			return err // NotFound
		}
		id, entry, err := getChild(tree, children, parentID, path[len(path)-1])
		if err != nil {
			return err // NotFound
		}
		if _, isDir := entry.(*DirEntry); !isDir {
			return fserr.NotDir // Test coverage: needs file implementation
		}

		return rmdir(tree, children, parentID, id) // nil or NotEmpty
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
		oldParentID, _, err := lookup(tree, children, oldPath[:len(oldPath)-1])
		if err != nil {
			return err // Returns NotFound if the source path or a parent of the destination path does not exist.
		}
		oldEntryID, oldEntry, err := getChild(tree, children, oldParentID, oldPath[len(oldPath)-1])
		if err != nil {
			return err // Returns NotFound if the source path or a parent of the destination path does not exist.
		}

		// Check for no-op rename (source = target) for existing paths
		if Equals(oldPath, newPath) {
			return nil // If oldPath and newPath exist and are the same, no operation is performed (success).
		}

		// Lookup destination: parent and, if any, entry
		newParentID, newParent, err := lookup(tree, children, newPath[:len(newPath)-1])
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

		if _, isDir := newParent.(*DirEntry); !isDir {
			return fserr.NotDir // Returns NotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
		}

		// Detect loop renames (directory to its own subdirectory)
		if len(oldPath) < len(newPath) && Equals(oldPath, newPath[:len(oldPath)]) {
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

		return renameDirectory(tree, children, oldParentID, oldEntryID, oldEntry, newParentID, newPath[len(newPath)-1])
	})
}
