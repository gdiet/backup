package main

import (
	"log"
)

// Create creates and opens a file.
//   - Returns -fuse.ENOENT if the parent path does not exist.
//   - Returns -fuse.ENOTDIR if the parent path is not a directory.
//   - Returns -fuse.EEXIST if the file already exists.
//
// Flags and mode are ignored until we find a use case where we need them.
func (f *fs) Create(path string, flags int, mode uint32) (int, uint64) {
	log.Printf("Create flags %d mode %d - %s", flags, mode, path)
	return 0, 47
}

// Open opens a file.
//   - Returns -fuse.ENOENT if the path does not exist.
//   - Returns -fuse.EISDIR if the path is a directory.
//
// Flags and mode are ignored until we find a use case where we need them.
func (f *fs) Open(path string, flags int) (int, uint64) {
	log.Printf("Open flags %d - %s", flags, path)

	return 0, 47
}
