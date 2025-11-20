package main

import (
	"log"
)

// Create creates and opens a file.
//   - Returns -fuse.ENOENT if the parent path does not exist.
//   - Returns -fuse.ENOTDIR if the parent path is not a directory.
//   - Returns -fuse.EEXIST if the file already exists.
//
// Let's ignore flags here for now, or maybe just assert that they are zero.
func (f *fs) Create(path string, flags int, mode uint32) (int, uint64) {
	log.Printf("Create flags %d mode %d - %s", flags, mode, path)
	return 0, 47
}

// Open opens a file.
//   - Returns -fuse.ENOENT if the path does not exist.
//   - Returns -fuse.EISDIR if the path is a directory.
//
// Let's ignore flags here for now, just check for O_APPEND and if found return an error. Reason:
//
// https://libfuse.github.io/doxygen/structfuse__operations.html#a08a085fceedd8770e3290a80aa9645ac
//
// Creation (O_CREAT, O_EXCL, O_NOCTTY) flags will be filtered out / handled by the kernel. ...
//
// The alternative would be to support e.g.:
//   - fuse.O_CREAT - create file if it does not exist.
//   - fuse.O_EXCL - fail if the file already exists (only used with O_CREAT).
//   - fuse.O_TRUNC - truncate file to size 0 when opened.
//   - fuse.O_APPEND - append data to the end of the file when writing.
//
// and so on...
func (f *fs) Open(path string, flags int) (int, uint64) {
	log.Printf("Open flags %d - %s", flags, path)

	return 0, 47
}
