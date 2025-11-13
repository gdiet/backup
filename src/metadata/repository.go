package metadata

import (
	"backup/src/metadata/internal"
	"fmt"
	"os"

	"go.etcd.io/bbolt"
)

// planned bbolt buckets for the file system metadata:
// - tree entries (id -> dirEntry, fileEntry)
// - children (parentID|childID -> {})
// - data entries (len|hash -> dataEntry)
// - free areas (start -> length)
// - context (string -> string, e.g. version information)
// entry id: uint64 (easier to handle than int64 in bbolt keys)
// hash: blake3 256-bit hash, represented as 32 bytes

// estimated average size of a file in the bbolt repository is 160 bytes
// tree entries:
// 8 key -> 1 type 8 time 40 dref 23 name = 72
// children:
// 16 key -> {} = 16
// data entries:
// 40 key -> 8 refs 16 area = 24
// bbolt management: 3*16 = 48

const (
	bucketTree      = "tree"
	bucketChildren  = "children"
	bucketData      = "data"
	bucketFreeAreas = "free"
	bucketContext   = "context"
)

type Repository struct {
	db *bbolt.DB
}

// NewRepository creates or opens a repository at the specified file path, initializing buckets as needed.
func NewRepository(filePath string) (*Repository, error) {
	db, err := bbolt.Open(filePath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt database: %w", err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		// Create buckets if needed
		for _, bucketName := range []string{bucketTree, bucketChildren, bucketData, bucketFreeAreas, bucketContext} {
			_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
			if err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", bucketName, err)
			}
		}
		// Initialize buckets
		internal.InitializeFreeAreas(tx.Bucket([]byte(bucketFreeAreas)))
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	r := &Repository{db: db}
	// FIXME temporary setup for testing, move to dev/prod switched initialization
	// TODO: Only initialize test directories when needed, not for every repository
	/*
		id, err := r.Mkdir(0, "testing")
		if err == nil {
			_, err = r.Mkdir(id, "hello")
		}
		if err == nil {
			_, err = r.Mkdir(id, "world")
		}
		if err != nil {
			db.Close()
			return nil, err
		}
	*/
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
		tree := tx.Bucket([]byte(bucketTree))
		children := tx.Bucket([]byte(bucketChildren))
		data := tx.Bucket([]byte(bucketData))
		freeAreas := tx.Bucket([]byte(bucketFreeAreas))
		return internal.Unlink(tree, children, data, freeAreas, id)
	})
	return err
}

// Mkdir creates a new directory. It does not check whether the parent exists.
// Returns the ID of the newly created directory.
// Returns os.ErrExist if a child with the same name already exists under the specified parent.
func (r *Repository) Mkdir(parent uint64, name string) (uint64, error) {
	var id uint64
	var err error
	err = r.db.Update(func(tx *bbolt.Tx) error {
		tree := tx.Bucket([]byte(bucketTree))
		children := tx.Bucket([]byte(bucketChildren))
		id, err = internal.Mkdir(tree, children, parent, name)
		return err
	})
	return id, err
}

// Readdir lists the entries under the specified directory.
// Returns os.ErrNotExist if the directory does not exist.
func (r *Repository) Readdir(path []string) (entries []internal.TreeEntry, err error) {
	err = r.db.View(func(tx *bbolt.Tx) error {
		tree := tx.Bucket([]byte(bucketTree))
		children := tx.Bucket([]byte(bucketChildren))

		// Lookup the directory ID for the given path
		id, entry, err := internal.Lookup(tree, children, path)
		if err != nil {
			return err
		}

		// Ensure the target is a directory
		if _, isDir := entry.(*internal.DirEntry); !isDir {
			return os.ErrNotExist
		}

		// Read the directory contents
		entries, err = internal.ReaddirForID(tree, children, id)
		return err
	})
	return entries, err
}

// ReaddirForID lists the entries under the specified directory. It does not check whether the directory exists.
// TODO check whether we need both Readdir and ReaddirForID
func (r *Repository) ReaddirForID(id uint64) (entries []internal.TreeEntry, err error) {
	err = r.db.View(func(tx *bbolt.Tx) error {
		tree := tx.Bucket([]byte(bucketTree))
		children := tx.Bucket([]byte(bucketChildren))
		entries, err = internal.ReaddirForID(tree, children, id)
		return err
	})
	return entries, err
}

// Close closes the repository database.
func (r *Repository) Close() error {
	return r.db.Close()
}
