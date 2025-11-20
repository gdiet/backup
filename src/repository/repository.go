package repository

import (
	"backup/src/repository/internal"
	"fmt"

	"go.etcd.io/bbolt"
)

const (
	treeKey      = "tree"
	childrenKey  = "children"
	dataKey      = "data"
	freeAreasKey = "free"
)

type Repository struct {
	db           *bbolt.DB
	treeKey      []byte
	childrenKey  []byte
	dataKey      []byte
	freeAreasKey []byte
}

// NewRepository creates or opens a repository at the specified file path.
func NewRepository(filePath string) (*Repository, error) {
	return newRepository(filePath, []byte(treeKey), []byte(childrenKey), []byte(dataKey), []byte(freeAreasKey))
}

// newRepository can be used for testing with custom bucket keys.
func newRepository(filePath string, treeKey, childrenKey, dataKey, freeAreasKey []byte) (*Repository, error) {
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
	r := &Repository{db: db, treeKey: treeKey, childrenKey: childrenKey, dataKey: dataKey, freeAreasKey: freeAreasKey}
	return r, nil
}

// Close closes the repository.
func (r *Repository) Close() error {
	return r.db.Close()
}
