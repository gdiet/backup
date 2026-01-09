package handles

import (
	"backup/src/fserr"
	"backup/src/meta"
	"strings"
	"sync"
)

type node struct {
	mu      sync.RWMutex
	handles []uint64
}

type Handles struct {
	mu     sync.RWMutex
	meta   meta.Metadata
	paths  map[string]uint64
	inodes map[uint64]*node
	lastId uint64
}

func (h *Handles) Create(path []string) (uint64, error) {
	file := strings.Join(path, "/")

	// Write lock parent.
	h.mu.Lock()

	// Ensure no inode exists at path.
	if _, exists := h.paths[file]; exists {
		h.mu.Unlock()
		return 0, fserr.Exists
	}

	// Create empty file entry in DB, returns inode.
	inode, err := h.meta.Mkdir(path) // FIXME replace with Mkfile
	if err != nil {
		h.mu.Unlock()
		return 0, err
	}

	// Add path -> inode entry.
	h.paths[file] = inode

	// Generate handle.
	h.lastId++
	handle := h.lastId

	// Create inode.
	node := &node{mu: sync.RWMutex{}, handles: []uint64{handle}}
	h.inodes[inode] = node

	// Lock inode.
	node.mu.Lock()
	defer node.mu.Unlock()

	// Unlock parent.
	h.mu.Unlock()

	// TODO inode -> cache: Add cache with empty base file
	// This can take some time...
	// TODO oder auch nicht - es ist schnell. Brauchen wir ein zweistufiges Lock?

	// Return handle
	return handle, nil
}
