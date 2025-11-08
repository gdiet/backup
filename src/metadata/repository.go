package metadata

import (
	"encoding/binary"
	"fmt"
	"math"

	"go.etcd.io/bbolt"
)

// planned bbolt buckets for the file system metadata:
// - tree entries (id -> dirEntry, fileEntry)
// - children (parentID|childID -> {})
// - data entries (len|hash -> dataEntry)
// - free areas (start -> length)
// - context (string -> string, e.g. version information)
// hash: blake3 256-bit hash, represented as 32 bytes

var (
	bucketTreeEntries = []byte("tree_entries")
	bucketChildren    = []byte("children")
	bucketDataEntries = []byte("data_entries")
	bucketFreeAreas   = []byte("free_areas")
	bucketContext     = []byte("context")
)

type repository struct {
	db *bbolt.DB
}

func (r *repository) mkdir(parent int64) {
	r.db.Update(func(tx *bbolt.Tx) error {
		childBucket := tx.Bucket(bucketChildren)
		childCursor := childBucket.Cursor()
		childCursor.Seek(int64bytes(parent))
		// FIXME implement mkdir logic
		return nil
	})
}

// NewRepository creates or opens a repository at the specified file path.
// If the free areas bucket doesn't exist, it initializes it with a single area covering 0...MaxInt64.
func NewRepository(filePath string) (*repository, error) {
	db, err := bbolt.Open(filePath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt database: %w", err)
	}

	repo := &repository{db: db}

	// Initialize buckets and free areas if needed
	err = db.Update(func(tx *bbolt.Tx) error {
		// Create all buckets if they don't exist
		for _, bucketName := range [][]byte{
			bucketTreeEntries,
			bucketChildren,
			bucketDataEntries,
			bucketFreeAreas,
			bucketContext,
		} {
			_, err := tx.CreateBucketIfNotExists(bucketName)
			if err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", bucketName, err)
			}
		}

		// Initialize free areas if empty
		freeAreasBucket := tx.Bucket(bucketFreeAreas)
		if freeAreasBucket.Stats().KeyN == 0 {
			// Add initial free area: 0 -> MaxInt64
			err := freeAreasBucket.Put(int64bytes(0), int64bytes(math.MaxInt64))
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

// int64bytes converts an int64 to a byte slice key for bbolt.
func int64bytes(id int64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(id))
	return key
}
