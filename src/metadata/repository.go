package metadata

import (
	"backup/src/metadata/internal"
	"fmt"

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
	return &Repository{db: db}, nil
}

// Mkdir creates a new directory. It does not check whether the parent exists.
// Returns os.ErrExist if a child with the same name already exists under the specified parent.
func (r *Repository) Mkdir(parent uint64, name string) error {
	err := r.db.Update(func(tx *bbolt.Tx) error {
		tree := tx.Bucket([]byte(bucketTree))
		children := tx.Bucket([]byte(bucketChildren))
		return internal.Mkdir(tree, children, parent, name)
	})
	return err
}

// Readdir lists the entries under the specified parent directory. It does not check whether the parent exists.
func (r *Repository) Readdir(parent uint64) (entries []internal.TreeEntry, err error) {
	err = r.db.View(func(tx *bbolt.Tx) error {
		tree := tx.Bucket([]byte(bucketTree))
		children := tx.Bucket([]byte(bucketChildren))
		entries, err = internal.Readdir(tree, children, parent)
		return err
	})
	return entries, err
}

// Close closes the repository database.
func (r *Repository) Close() error {
	return r.db.Close()
}
