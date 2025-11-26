package meta

import (
	"backup/src/meta/internal"
	"fmt"

	"go.etcd.io/bbolt"
)

const (
	treeKey      = "tree"
	childrenKey  = "children"
	dataKey      = "data"
	freeAreasKey = "free"
)

type Metadata struct {
	db           *bbolt.DB
	treeKey      []byte
	childrenKey  []byte
	dataKey      []byte
	freeAreasKey []byte
}

// NewMetadata creates or opens a repository at the specified file path.
func NewMetadata(filePath string) (*Metadata, error) {
	return newMetadata(filePath, []byte(treeKey), []byte(childrenKey), []byte(dataKey), []byte(freeAreasKey))
}

// newMetadata can be used for testing with custom bucket keys.
func newMetadata(filePath string, treeKey, childrenKey, dataKey, freeAreasKey []byte) (*Metadata, error) {
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
		// Initialize buckets FIXME unhandled error
		internal.InitializeFreeAreas(tx.Bucket(freeAreasKey))
		return nil
	})
	if err != nil {
		db.Close() // FIXME unhandled error
		return nil, err
	}
	r := &Metadata{db: db, treeKey: treeKey, childrenKey: childrenKey, dataKey: dataKey, freeAreasKey: freeAreasKey}
	return r, nil
}

// Close closes the repository.
func (r *Metadata) Close() error {
	return r.db.Close()
}
