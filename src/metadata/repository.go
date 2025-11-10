package metadata

import (
	"backup/src/util"
	"fmt"
	"math"
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
	bucketTreeEntries = "tree_entries"
	bucketChildren    = "children"
	bucketDataEntries = "data_entries"
	bucketFreeAreas   = "free_areas"
	bucketContext     = "context"
)

type repository struct {
	db *bbolt.DB
}

func (r *repository) mkdir(parent uint64, name string) error {
	err := r.db.Update(func(tx *bbolt.Tx) error {
		tree := tx.Bucket([]byte(bucketTreeEntries))
		children := tx.Bucket([]byte(bucketChildren))

		// check if child with name exists
		cursor := children.Cursor()
		for k, _ := cursor.Seek(u64b(parent)); k != nil && b64u(k) != parent; k, _ = cursor.Next() {
			util.Assert(len(k) == 16, "invalid child key length")
			bytes := tree.Get(k[8:])
			if bytes == nil {
				continue
			}
			entry, err := treeEntryFromBytes(bytes)
			if err != nil {
				return err
			}
			if entry.getName() == name {
				return os.ErrExist
			}
		}

		// get next available tree entries ID
		var nextID uint64
		if bytes, _ := tree.Cursor().Last(); bytes != nil {
			nextID = b64u(bytes) + 1
		}

		// write new dir entry
		return tree.Put(u64b(nextID), (&dirEntry{name: name}).toBytes())
	})
	return err
}

// NewRepository creates or opens a repository at the specified file path.
// Uses the standard bucket names defined in const block.
// If the free areas bucket doesn't exist, it initializes it with a single area covering 0...MaxInt64.
func NewRepository(filePath string) (*repository, error) {
	return NewRepositoryWithBuckets(filePath,
		bucketTreeEntries,
		bucketChildren,
		bucketDataEntries,
		bucketFreeAreas,
		bucketContext)
}

// NewRepositoryWithBuckets creates or opens a repository with custom bucket names.
// This function is useful for testing error conditions or using alternative bucket names.
// If the free areas bucket doesn't exist, it initializes it with a single area covering 0...MaxInt64.
func NewRepositoryWithBuckets(filePath string, treeEntriesBucket, childrenBucket, dataEntriesBucket, freeAreasBucket, contextBucket string) (*repository, error) {
	db, err := bbolt.Open(filePath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt database: %w", err)
	}

	repo := &repository{db: db}

	// Initialize buckets and free areas if needed
	err = db.Update(func(tx *bbolt.Tx) error {
		// Create all buckets if they don't exist
		for _, bucketName := range []string{
			treeEntriesBucket,
			childrenBucket,
			dataEntriesBucket,
			freeAreasBucket,
			contextBucket,
		} {
			_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
			if err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", bucketName, err)
			}
		}

		// Initialize free areas if empty
		freeAreasBucketHandle := tx.Bucket([]byte(freeAreasBucket))
		if freeAreasBucketHandle.Stats().KeyN == 0 {
			// Add initial free area: 0 -> MaxInt64
			err := freeAreasBucketHandle.Put(u64b(0), u64b(math.MaxInt64))
			if err != nil {
				return fmt.Errorf("failed to initialize free areas: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		db.Close()
		return nil, err
	}

	return repo, nil
}

// Close closes the repository database.
func (r *repository) Close() error {
	return r.db.Close()
}
