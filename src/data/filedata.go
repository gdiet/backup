package data

import (
	"backup/src/cache"
	"backup/src/fserr"
	"path/filepath"
	"strconv"
	"sync"
)

// FileData manages file contents.
// Methods assume the maps are already initialized.
type FileData struct {
	cachePath string
	open      map[uint64]cache.Cache
	closing   map[uint64][]cache.Cache
	lock      sync.RWMutex
}

type FileHandle struct {
	ID    uint64
	Cache cache.Cache
}

// Add adds a new handle. FIXME needs base file which can be a closing handle.
func Add(h *FileData, id uint64) error {
	h.lock.Lock()
	defer h.lock.Unlock()
	if _, exists := h.open[id]; exists {
		return fserr.Exists
	}
	path := filepath.Join(h.cachePath, strconv.FormatUint(id, 10))
	h.open[id] = cache.NewCache(path, nil)
	return nil
}

// Close queues a handle for removal.
func Close(h *FileData, id uint64) error {
	h.lock.Lock()
	defer h.lock.Unlock()
	handle, exists := h.open[id]
	if !exists {
		return fserr.NotFound
	}
	delete(h.open, id)
	h.closing[id] = append(h.closing[id], handle)
	return nil
}

// read -> not found would mean look into repository
// truncate
// write

// func Get(h *Handles, id uint64) (cache.Cache, bool) {
// 	h.lock.RLock()
// 	defer h.lock.RUnlock()
// 	c, exists := h.open[id]
// 	return c, exists
// }
