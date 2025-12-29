package fuse

import (
	"backup/src/fserr"
	"backup/src/meta"
	"backup/src/util"
	"errors"
	"log"

	"github.com/winfsp/cgofuse/fuse"
)

// Create creates and opens a file.
//
//	Returns -fuse.ENOENT if the parent path does not exist.
//	Returns -fuse.ENOTDIR if the parent path is not a directory.
//	Returns -fuse.EEXIST if the file already exists.
//
// 'flags' and 'mode' are ignored until we find a use case where we need them.
func (f *FS) Create(path string, flags int, mode uint32) (int, uint64) {
	log.Printf("Create flags %d mode %d - %s", flags, mode, path)

	id, err := f.repo.Create(partsFrom(path))
	switch {
	case err == nil:
		// continue
	case errors.Is(err, fserr.NotFound):
		return -fuse.ENOENT, 0
	case errors.Is(err, fserr.NotDir):
		return -fuse.ENOTDIR, 0
	case errors.Is(err, fserr.Exists):
		return -fuse.EEXIST, 0
	default:
		util.AssertionFailedf("unexpected error %v in Create", err)
		return -fuse.EIO, 0
	}

	log.Printf("Create file id %d - %s", id, path)
	return 0, id
}

// Open opens a file.
//
//	Returns -fuse.ENOENT if the path does not exist.
//	Returns -fuse.EISDIR if the path is a directory.
//
// 'flags' is ignored until we find a use case where we need it.
func (f *FS) Open(path string, flags int) (int, uint64) {
	log.Printf("Open flags %d - %s", flags, path)

	id, entry, err := f.repo.Lookup(partsFrom(path))
	switch {
	case err == nil:
		// continue
	case errors.Is(err, fserr.NotFound):
		return -fuse.ENOENT, 0
	default:
		util.AssertionFailedf("unexpected error %v in Open: repo.Lookup", err)
		return -fuse.EIO, 0
	}
	switch e := entry.(type) {
	case *meta.FileEntry:
		// continue
	case *meta.DirEntry:
		return -fuse.EISDIR, 0
	default:
		util.AssertionFailedf("unexpected entry type %T in Open", e)
		return -fuse.ENOENT, 0
	}

	log.Printf("Open file id %d - %s", id, path)
	_, err = f.repo.Open(id)
	if err != nil {
		util.AssertionFailedf("unexpected error %v in Open: repo.Open", err)
		return -fuse.EIO, 0
	}
	return 0, id
}
