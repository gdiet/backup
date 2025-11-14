package metadata

import (
	internal "backup/src/metadata/notinternal"
	"fmt"
	"log"
	"os"

	"go.etcd.io/bbolt"
)

var ( // var for technical reasons, DO NOT MUTATE
	treeKey      = []byte("tree")
	childrenKey  = []byte("children")
	dataKey      = []byte("data")
	freeAreasKey = []byte("free")
)

type Repository struct {
	db *bbolt.DB
}

// NewRepository creates or opens a repository at the specified file path.
func NewRepository(filePath string) (*Repository, error) {
	db, err := bbolt.Open(filePath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt database: %w", err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		// Create buckets if needed
		for _, bucketKey := range [][]byte{treeKey, childrenKey, dataKey, freeAreasKey} {
			_, err := tx.CreateBucketIfNotExists(bucketKey)
			if err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", bucketKey, err)
			}
		}
		// Initialize buckets
		internal.InitializeFreeAreas(tx.Bucket(freeAreasKey))
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	r := &Repository{db: db}
	return r, nil
}

// Rename renames a directory or file, moving it between directories if required.
// Returns ENOENT if id does not exist.
// Returns ENOENT if parent of newpath does not exist.
// Returns ENOTDIR if parent of newpath is a file.
// Returns EISDIR if id is a file and newpath is a dir.
// Returns ENOTEMPTY if newpath is a nonempty directory.
/*
   If newpath already exists, it will be atomically replaced, so that
   there is no point at which another process attempting to access
   newpath will find it missing.  However, there will probably be a
   window in which both oldpath and newpath refer to the file being
   renamed.

   If oldpath and newpath are existing hard links referring to the
   same file, then rename() does nothing, and returns a success
   status.

   If newpath exists but the operation fails for some reason,
   rename() guarantees to leave an instance of newpath in place.

   oldpath can specify a directory.  In this case, newpath must
   either not exist, or it must specify an empty directory.

   If oldpath refers to a symbolic link, the link is renamed; if
   newpath refers to a symbolic link, the link will be overwritten.
*/
func (r *Repository) Rename(id uint64, newPath []string) error {
	panic("not implemented")
}

// Unlink deletes a file or directory from the filesystem.
// Returns os.ErrNotExist if the entry does not exist.
// Returns syscall.ENOTEMPTY if trying to delete a non-empty directory.
func (r *Repository) Unlink(id uint64) error {
	err := r.db.Update(func(tx *bbolt.Tx) error {
		tree := tx.Bucket(treeKey)
		children := tx.Bucket(childrenKey)
		data := tx.Bucket(dataKey)
		freeAreas := tx.Bucket(freeAreasKey)
		return internal.Unlink(tree, children, data, freeAreas, internal.U64b(id))
	})
	return err
}

// Mkdir creates a new directory. It does not check whether the parent exists.
// Returns the ID of the newly created directory.
// Returns os.ErrExist if a child with the same name already exists under the specified parent.
func (r *Repository) Mkdir(parent uint64, name string) (uint64, error) {
	var idBytes []byte
	var err error
	err = r.db.Update(func(tx *bbolt.Tx) error {
		tree := tx.Bucket(treeKey)
		children := internal.WrapBucket(tx.Bucket(childrenKey))
		idBytes, err = internal.Mkdir(tree, children, internal.U64b(parent), name)
		return err
	})
	if err != nil {
		return 0, err
	}
	return internal.B64u(idBytes), nil
}

// Readdir lists the entries under the specified directory.
// Returns os.ErrNotExist if the directory does not exist.
func (r *Repository) Readdir(path []string) (entries []internal.TreeEntry, err error) {
	err = r.db.View(func(tx *bbolt.Tx) error {
		tree := tx.Bucket(treeKey)
		children := internal.WrapBucket(tx.Bucket(childrenKey))

		// Lookup the directory ID for the given path
		idBytes, entry, err := internal.Lookup(tree, children, path)
		log.Printf("Lookup result for path %v: idBytes=%v, entry=%v, err=%v", path, idBytes, entry, err)
		if err != nil {
			return err
		}

		// Ensure the target is a directory
		if _, isDir := entry.(*internal.DirEntry); !isDir {
			return os.ErrNotExist
		}

		// Read the directory contents
		entries, err = internal.ReaddirForID(tree, children, idBytes)
		return err
	})
	return entries, err
}

// ReaddirForID lists the entries under the specified directory. It does not check whether the directory exists.
// TODO check whether we need both Readdir and ReaddirForID
func (r *Repository) ReaddirForID(id uint64) (entries []internal.TreeEntry, err error) {
	err = r.db.View(func(tx *bbolt.Tx) error {
		tree := tx.Bucket(treeKey)
		children := internal.WrapBucket(tx.Bucket(childrenKey))
		entries, err = internal.ReaddirForID(tree, children, internal.U64b(id))
		return err
	})
	return entries, err
}

// Lookup looks up a path and returns the ID and tree entry.
// Returns os.ErrNotExist if the path does not exist.
func (r *Repository) Lookup(path []string) (id uint64, entry internal.TreeEntry, err error) {
	err = r.db.View(func(tx *bbolt.Tx) error {
		tree := tx.Bucket(treeKey)
		children := internal.WrapBucket(tx.Bucket(childrenKey))
		var idBytes []byte
		if idBytes, entry, err = internal.Lookup(tree, children, path); err != nil {
			return err
		}
		id = internal.B64u(idBytes)
		return nil
	})
	return id, entry, err
}

// Close closes the repository database.
func (r *Repository) Close() error {
	return r.db.Close()
}
