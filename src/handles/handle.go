package handles

import (
	"backup/src/cache"
	"backup/src/fserr"
	"backup/src/meta"
	"backup/src/util"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
)

type node struct {
	mu      sync.RWMutex // FIXME check if needed
	handles []uint64
	cache   cache.Cache
}

type Handles struct {
	mu       sync.RWMutex
	cacheDir string
	meta     meta.Metadata
	paths    map[string]uint64
	nodes    map[uint64]*node
	lastId   uint64
}

func (h *Handles) Create(path []string) (uint64, error) {
	file := strings.Join(path, "/")

	// Write lock parent.
	h.mu.Lock()
	defer h.mu.Unlock()

	// Ensure no inode exists at path.
	if _, exists := h.paths[file]; exists {
		return 0, fserr.Exists
	}

	// Create empty file entry in DB, returns inode.
	inode, err := h.meta.Mkdir(path) // FIXME replace with Mkfile
	if err != nil {
		return 0, err
	}

	// Add path -> inode entry.
	h.paths[file] = inode

	// Generate handle.
	h.lastId++
	handle := h.lastId

	// Add inode -> cache with empty base file
	node := &node{
		mu:      sync.RWMutex{},
		handles: []uint64{handle},
		cache:   cache.NewCache(filepath.Join(h.cacheDir, fmt.Sprint(inode)), &cache.EmptyBaseFile{}),
	}
	h.nodes[inode] = node

	// Return handle
	return handle, nil
}

func (h *Handles) Open(path []string) (uint64, error) {
	file := strings.Join(path, "/")

	// Write lock parent.
	h.mu.Lock()
	defer h.mu.Unlock()

	// Check whether inode exists at path.
	inode, exists := h.paths[file]
	if exists {
		node, exists := h.nodes[inode]
		if !exists {
			util.AssertionFailedf("for path %s, inode %d, node not found in nodes map", file, inode)
			return 0, fserr.IO()
		}
		// Generate handle.
		h.lastId++
		handle := h.lastId

		// Add handle to node.
		node.handles = append(node.handles, handle)

		// Return handle
		return handle, nil
	}

	// Look up file entry in DB, returns inode.
	inode, treeEntry, err := h.meta.Lookup(path)
	if err != nil {
		return 0, err
	}

	// Ensure it's a file.
	switch treeEntry.(type) {
	case *meta.FileEntry:
		// continue
	case *meta.DirEntry:
		return 0, fserr.IsDir
	default:
		util.AssertionFailedf("unexpected entry type %T in Open", treeEntry)
		return 0, fserr.Invalid
	}

	// Add path -> inode entry.
	h.paths[file] = inode

	// Generate handle.
	h.lastId++
	handle := h.lastId

	// Add inode -> cache with base file from file entry.
	node := &node{
		mu:      sync.RWMutex{},
		handles: []uint64{handle},
		// FIXME replace EmptyBaseFile with actual base file from file entry
		cache: cache.NewCache(filepath.Join(h.cacheDir, fmt.Sprint(inode)), &cache.EmptyBaseFile{}),
	}
	h.nodes[inode] = node

	// Return handle
	return handle, nil
}
