package fuse

import (
	"backup/src/fserr"
	"backup/src/meta"
	"backup/src/util"
	"log"

	"github.com/winfsp/cgofuse/fuse"
)

// Create creates and opens a file.
//   - Returns -fuse.ENOENT if the parent path does not exist.
//   - Returns -fuse.ENOTDIR if the parent path is not a directory.
//   - Returns -fuse.EEXIST if the file already exists.
//
// Flags and mode are ignored until we find a use case where we need them.
func (f *FuseFS) Create(path string, flags int, mode uint32) (int, uint64) {
	log.Printf("Create flags %d mode %d - %s", flags, mode, path)

	id, err := f.repo.Mkfile(partsFrom(path))
	switch err {
	case nil:
		return 0, id
	case fserr.ErrNotFound:
		return -fuse.ENOENT, 0
	case fserr.ErrNotDir:
		return -fuse.ENOTDIR, 0
	case fserr.ErrExists:
		return -fuse.EEXIST, 0
	default:
		util.AssertionFailedf("unexpected error %v in Create", err)
		return -fuse.EIO, 0
	}
}

// Open opens a file.
//   - Returns -fuse.ENOENT if the path does not exist.
//   - Returns -fuse.EISDIR if the path is a directory.
//
// Flags and mode are ignored until we find a use case where we need them.
func (f *FuseFS) Open(path string, flags int) (int, uint64) {
	log.Printf("Open flags %d - %s", flags, path)

	id, entry, err := f.repo.Lookup(partsFrom(path))
	if err == fserr.ErrNotFound {
		return -fuse.ENOENT, 0
	}
	if err != nil {
		util.AssertionFailedf("unexpected error %v in Open", err)
		return -fuse.EIO, 0
	}
	switch e := entry.(type) {
	case *meta.DirEntry:
		return -fuse.EISDIR, 0
	case *meta.FileEntry:
		log.Printf("Opened file %d - %s", id, path)
		return 0, id // this is the OK case :)
	default:
		util.AssertionFailedf("unexpected entry type %T in Open", e)
		return -fuse.ENOENT, 0
	}
}
