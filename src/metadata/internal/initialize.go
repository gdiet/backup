package internal

import (
	"fmt"
	"math"

	"go.etcd.io/bbolt"
)

func InitializeFreeAreas(freeAreas *bbolt.Bucket) error {
	if freeAreas.Stats().KeyN == 0 {
		// Add initial free area: 0 -> MaxInt64
		err := freeAreas.Put(U64b(0), U64b(math.MaxInt64))
		if err != nil {
			return fmt.Errorf("failed to initialize free areas: %w", err)
		}
	}
	return nil
}
