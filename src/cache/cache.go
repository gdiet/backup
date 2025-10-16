package cache

type Cache struct {
	// tiered             map[int]*Tiered
	// lastModifiedMillis map[int]int
	// fileLocks          map[int]*sync.RWMutex
	// cacheLock          sync.RWMutex
}

func (cache *Cache) Write(position int64, data Bytes) {
	panic("not implemented")
}
