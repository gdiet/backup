package meta

import (
	"backup/src/fserr"
	"math"

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
	db, err := bbolt.Open(repository+"/dedupfs.db", 0600, nil)
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
				return fserr.IO
			}
		}
		// Initialize free areas if needed
		freeAreas := tx.Bucket(m.freeAreasKey)
		firstKey, _ := freeAreas.Cursor().First()
		if len(firstKey) == 0 {
			// Add initial free area: 0 -> MaxInt64
			if err := freeAreas.Put(U64b(0), U64b(math.MaxInt64)); err != nil {
				return fserr.IO
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

// Lookup looks up a path and returns the ID and tree entry.
// Returns ErrNotFound if the path does not exist.
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
