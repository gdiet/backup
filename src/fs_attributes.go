package main

import (
	"backup/src/repository"
	"backup/src/util"
	"log"
	"os"
	"time"

	"github.com/winfsp/cgofuse/fuse"
)

// Getattr gets the attributes of a file or directory.
// https://man7.org/linux/man-pages/man2/stat.2.html
// Returns -fuse.ENOENT if the path does not exist.
// Returns -fuse.EIO on other errors.
func (f *fs) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	log.Printf("Getattr %s", path)

	// set basic stat, will be overwritten if it's a file
	stat.Mode = fuse.S_IFDIR | 0755
	stat.Mtim = fuse.NewTimespec(time.Now())
	stat.Nlink = 2
	stat.Uid = uint32(os.Getuid())
	stat.Gid = uint32(os.Getgid())

	parts := partsFrom(path)
	if len(parts) == 1 && parts[0] == "" { // Root directory
		return 0
	}

	_, entry, err := f.repo.Lookup(parts)
	switch err {
	case nil:
		// continue
	case repository.ErrNotFound:
		return -fuse.ENOENT
	default:
		util.AssertionFailedf("unexpected error %v in Getattr", err)
		return -fuse.EIO
	}

	switch entry := entry.(type) {
	case *repository.DirEntry:
		return 0
	case *repository.FileEntry:
		stat.Mode = fuse.S_IFREG | 0644
		stat.Size = entry.Size()
		return 0
	default:
		util.AssertionFailedf("unexpected entry type %T in Getattr", entry)
		return -fuse.ENOENT
	}
}

// Utimens changes the access and modification times of a file.
// FIXME not implemented yet.
func (f *fs) Utimens(path string, tmsp []fuse.Timespec) int {
	return -fuse.ENOSYS
}
