package internal

import (
	"math"

	"go.etcd.io/bbolt"
)

// InitializeFreeAreas initializes the free areas bucket if it is empty.
// It adds a single free area covering the entire addressable space.
func InitializeFreeAreas(freeAreas *bbolt.Bucket) error {
	if freeAreas.Stats().KeyN == 0 {
		// Add initial free area: 0 -> MaxInt64
		return freeAreas.Put(U64b(0), U64b(math.MaxInt64))
	}
	return nil
}
