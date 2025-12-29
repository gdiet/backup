package data

type Handles struct {
}

func (f *Handles) Create(id uint64) (Handle, error) {
	panic("implement me")
}

func (f *Handles) Get(id uint64) (Handle, error) {
	panic("implement me")
}

func (f *Handles) Release(id uint64) error {
	panic("implement me")
}

type Handle interface {
	Read(off int64, data []byte) error
	Write(off int64, data []byte) error
	Truncate(newSize int64) error
}

/*
Here is the plan:

Handles has open/close and read/write/truncate methods
Handles manages handles: fileId -> Handle
A handle can be open or closing
read/write/truncate operate on open handles only

When a handle is closed, it is moved to closing state
Closing handles are kept in memory until they are written to store
When a handle has been written to store, it no longer points to cache, but to base file in store

Open on a fileId that has a closing handle will use the closing handle as base file

*/

//type fileHandle struct {
//	// cache is nil if handle has not been written yet or is closed and written to store.
//	cache *cache.Cache
//	open  bool
//}

//// Handles manages file contents.
//// Methods assume the maps are already initialized.
//type Handles struct {
//	cachePath string
//	open      map[uint64]cache.Cache
//	closing   map[uint64][]cache.Cache
//	lock      sync.RWMutex
//}
//
//type Handle struct {
//	ID    uint64
//	Cache cache.Cache
//}
//
//// Add adds a new handle. FIXME needs base file which can be a closing handle.
//func Add(h *Handles, id uint64) error {
//	h.lock.Lock()
//	defer h.lock.Unlock()
//	if _, exists := h.open[id]; exists {
//		return fserr.Exists
//	}
//	path := filepath.Join(h.cachePath, strconv.FormatUint(id, 10))
//	h.open[id] = cache.NewCache(path, nil)
//	return nil
//}
//
//// Close queues a handle for removal.
//func Close(h *Handles, id uint64) error {
//	h.lock.Lock()
//	defer h.lock.Unlock()
//	handle, exists := h.open[id]
//	if !exists {
//		return fserr.NotFound
//	}
//	delete(h.open, id)
//	h.closing[id] = append(h.closing[id], handle)
//	return nil
//}
//
//// read -> not found would mean look into repository
//// truncate
//// write
//
//// func Get(h *Handles, id uint64) (cache.Cache, bool) {
//// 	h.lock.RLock()
//// 	defer h.lock.RUnlock()
//// 	c, exists := h.open[id]
//// 	return c, exists
//// }
